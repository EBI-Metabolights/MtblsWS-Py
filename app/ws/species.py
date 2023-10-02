import json
import os.path
from typing import List, Set

from flask import current_app as app, jsonify
from flask_restful import Resource
from flask_restful_swagger import swagger
from app.config import get_settings
from app.services.storage_service.storage_service import StorageService
from app.utils import MetabolightsDBException, metabolights_exception_handler
from app.ws.db.dbmanager import DBManager
from app.ws.db.schemes import RefSpeciesGroup, RefSpeciesMember, RefSpecy
from app.ws.redis.redis import get_redis_server
from app.ws.study.study_service import StudyService
from pydantic import BaseModel


class SpeciesTreeNode(BaseModel):
    name: str = ""
    level: int = 0
    
class SpeciesTreeLeaf(SpeciesTreeNode):
    size: int = 1
    
class SpeciesTreeParent(SpeciesTreeNode):
    children: List[SpeciesTreeNode] = []
    

    
class SpeciesTree(Resource):
    @swagger.operation(
        summary="Returns  Species as a tree",
        parameters=[
        ],
        responseMessages=[
            {
                "code": 200,
                "message": "OK."
            },
            {
                "code": 401,
                "message": "Unauthorized. Access to the resource requires user authentication."
            },
            {
                "code": 403,
                "message": "Forbidden. Access to the study is not allowed for this user."
            },
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist."
            }
        ]
    )
    @metabolights_exception_handler
    def get(self):
        key = get_settings().redis_cache.configuration.species_tree_cache_key
        try: 
            redis = get_redis_server()
            tree = redis.get_value(key)
            
            if tree:
                return json.loads(tree)
        except Exception:
            # no cache or invalid cache
            pass
        
        tree: SpeciesTreeParent = SpeciesTreeParent()
        tree.name = "Species"
        groups = {}
        
        try:
            with DBManager.get_instance().session_maker() as db_session:
                result = db_session.query(RefSpeciesGroup).all()
                
                if result:
                    for item in result:
                        # if item.parent == None:
                        groups[item.id] = SpeciesTreeParent()
                        
                        groups[item.id].name = item.name

                    for item in result:
                        if item.parent == None:
                            tree.children.append(groups[item.id])
                        else:
                            if item.parent.id in groups:
                                groups[item.parent.id].children.append(groups[item.id])
                            else:
                                print(f"Species group parent id is not found in species group tree: {item.parent.id}")
                    
                    speciesMemberList = result = db_session.query(RefSpeciesMember).all()
                    members_and_groups = {}
                    if speciesMemberList:
                        for item in speciesMemberList:
                            members_and_groups[item.id] = item.group_id
                            
                    speciesList = result = db_session.query(RefSpecy).all()
                    
                    if speciesList:
                        for item in speciesList:
                            if item.species_member:
                                
                                if item.species_member in members_and_groups:
                                    group_id = members_and_groups[item.species_member]
                                    if group_id in groups:
                                        obj = SpeciesTreeLeaf(name=item.species, size=1)
                                        groups[group_id].children.append(obj)
                                    else:
                                        print(f"Group id is not found in species group tree. Member id: {item.species_member} group id: {group_id}")  
                                else:
                                    print(f"Species member id is not found in  species group tree: {item.species_member}")    
                    
                    self.update_tree(tree)                    
        except Exception as e:
            raise MetabolightsDBException(message=f"Error while retreiving study from database: {str(e)}", exception=e)
        
        result_dict = tree.dict()
        result_str = json.dumps(result_dict)
        redis.set_value(key, result_str, ex=60*10)
        return jsonify(result_dict)
    
    def update_tree(self, tree: SpeciesTreeParent, level: int = 0):
            tree.level = level
            tree.children.sort(key=self.get_sort_key)
            empty_item_list = []
            for item in tree.children:
                if isinstance(item, SpeciesTreeParent):
                    sub_item: SpeciesTreeParent = item
                    if len(sub_item.children) == 0:
                        empty_item_list.append(sub_item)
                    else:
                        self.update_tree(item, tree.level + 1)
                else:
                    item.level = item.level + 1
            if empty_item_list:
                for empty_item in empty_item_list:
                    tree.children.remove(empty_item)
                    
    def get_sort_key(self, item: SpeciesTreeNode):
        if not item or not item.name:
            return ""
        return item.name