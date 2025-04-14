import logging
from xml.dom.minidom import Document

"""
Utils for Documents
"""

logger = logging.getLogger('wslog')

def dict_to_xml(tag, d):
    doc = Document()
    root = doc.createElement(tag)
    doc.appendChild(root)
    for key, val in d.items():
        if key.startswith('@'):
            root.setAttribute(key[1:], val)
        else:
            child = doc.createElement(key)
            child.appendChild(doc.createTextNode(val))
            root.appendChild(child)
    return doc

def create_generic_element(doc, root, item_name, item_value):
    generic_elem = doc.createElement(item_name)
    generic_elem.appendChild(doc.createTextNode(item_value))
    root.appendChild(generic_elem)
    return doc

def create_generic_element_attribute(doc, root, item_name, item_value, attr_name, attr_value):
    generic_elem = doc.createElement(item_name)
    if item_value is not None or item_value != '':
        generic_elem.appendChild(doc.createTextNode(item_value))
    generic_elem.setAttribute(attr_name, attr_value)
    root.appendChild(generic_elem)
    return doc
    
def main():
    #doc = Document()
    #doc = create_generic_element(doc, 'test-elem', '4666')
    #my_dict = {'person': {'@id': '123', 'name': 'John', 'age': '30', 'city': 'New York'}}
    #doc = dict_to_xml('person', my_dict['person'])
    doc = Document()
    root = doc.createElement('entry')
    doc.appendChild(root)
    doc = create_generic_element(doc, root, 'test-elem', '4666')
    doc = create_generic_element_attribute(doc, root, 'test-elem1', '74666', 'type', 'new')
    doc = create_generic_element_attribute(doc, root, 'test-elem2', '', 'type', 'new')
    xml_str = doc.toprettyxml(indent="  ")
    print("xml str - "+ xml_str)
    
if __name__ == "__main__":
    main()