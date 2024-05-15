import logging
from app.utils import MetabolightsDBException
from app.ws.db import models
from app.ws.db.dbmanager import DBManager
from app.ws.db.schemes import RefMetabolite


logger = logging.getLogger('wslog')

class CompoundService(object):
    instance = None
    db_manager = None
    
    @classmethod
    def get_instance(cls):
        if not cls.instance:
            cls.instance = CompoundService()
            cls.db_manager = DBManager.get_instance()
        return cls.instance
    
    def get_all_compounds(self):
        acc_list = []
        with DBManager.get_instance().session_maker() as db_session:
            accs = db_session.query(RefMetabolite.acc).all()
            for acc in accs:
                acc_list.append(''.join(acc))
        return acc_list
    
    def get_compound(self, compound_acc: str):
        with DBManager.get_instance().session_maker() as db_session:
            compound = db_session.query(RefMetabolite).filter(RefMetabolite.acc == compound_acc).first()

            if not compound:
                raise MetabolightsDBException(f"{compound_acc} does not exist")

            compound_model = models.MetaboLightsCompoundModel.from_orm(compound)
            return compound_model