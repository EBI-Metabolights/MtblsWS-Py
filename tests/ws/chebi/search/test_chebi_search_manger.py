import unittest

from app.ws.chebi.search.chebi_search_manager import ChebiSearchManager
from app.ws.chebi.search.curated_metabolite_table import CuratedMetaboliteTable
from app.ws.chebi.settings import ChebiWsSettings
from app.ws.chebi.wsproxy import ChebiWsProxy
from instance import config

curated_file_location = config.CURATED_METABOLITE_LIST_FILE_LOCATION


class ChebiWsSettingsFixture(ChebiWsSettings):

    def __init__(self):
        super().__init__()
        self.chebi_ws_wsdl = config.CHEBI_WS_WSDL
        self.chebi_ws_wsdl_service = config.CHEBI_WS_WSDL_SERVICE
        self.chebi_ws_wsdl_service_port = config.CHEBI_WS_WSDL_SERVICE_PORT
        self.chebi_ws_strict = config.CHEBI_WS_STRICT
        self.chebi_ws_xml_huge_tree = config.CHEBI_WS_XML_HUGE_TREE
        self.chebi_ws_service_binding_log_level = config.CHEBI_WS_WSDL_SERVICE_BINDING_LOG_LEVEL


class ChebiSearchManagerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.settings = ChebiWsSettingsFixture()
        cls.curated_table = CuratedMetaboliteTable(curated_file_location)
        cls.proxy = ChebiWsProxy(settings=cls.settings)
        cls.search_manager = ChebiSearchManager(ws_proxy=cls.proxy, curated_metabolite_table=cls.curated_table)

    @classmethod
    def tearDownClass(cls):
        pass

    def test_search_by_database_id_success_01(self):
        search_type = "databaseId"
        search_value = "CHEBI:10225"
        expected_value = "CHEBI:10225"
        result = self.search_manager.search_by_type(search_type, search_value)
        self.assertIsNotNone(result)
        self.assertIsNotNone(result.content)
        self.assertEqual(1, len(result.content))
        self.assertEqual(expected_value, result.content[0].databaseId)

    def test_search_by_database_id_success_02(self):
        search_type = "databaseId"
        search_value = "CHEBI:102251"
        expected_value = "CHEBI:102251"
        result = self.search_manager.search_by_type(search_type, search_value)
        self.assertIsNotNone(result)
        self.assertIsNotNone(result.content)
        self.assertEqual(1, len(result.content))
        self.assertEqual(expected_value, result.content[0].databaseId)

    def test_search_by_database_id_not_found_03(self):
        search_type = "databaseId"
        search_value = "CHEBI:1022512"
        result = self.search_manager.search_by_type(search_type, search_value)
        self.assertIsNotNone(result)
        self.assertIsNotNone(result.content)
        self.assertIsNotNone(result.message)
        self.assertEqual(0, len(result.content))

    def test_search_by_database_id_search_error_01(self):
        search_type = "databaseId"
        search_value = "CHEBI:1022512x"
        result = self.search_manager.search_by_type(search_type, search_value)
        self.assertIsNotNone(result)
        self.assertIsNotNone(result.content)
        self.assertIsNotNone(result.message)
        self.assertEqual(0, len(result.content))

    def test_search_by_name_success_synonym_01(self):
        search_type = "name"
        search_value = "9-Cis,11-Trans,13-Trans-Octadecatrienoic Acid"
        expected_value = "(9Z,11E,13E)-octadeca-9,11,13-trienoic acid"
        result = self.search_manager.search_by_type(search_type, search_value)
        self.assertIsNotNone(result)
        self.assertIsNotNone(result.content)
        self.assertEqual(1, len(result.content))
        self.assertEqual(expected_value, result.content[0].name)

    def test_search_by_name_success_iupac_name_01(self):
        search_type = "name"
        search_value = "(5S)-5-hydroxy-1-(4-hydroxy-3-methoxyphenyl)decan-3-one"
        expected_value = "gingerol"
        result = self.search_manager.search_by_type(search_type, search_value)
        self.assertIsNotNone(result)
        self.assertIsNotNone(result.content)
        self.assertEqual(1, len(result.content))
        self.assertEqual(expected_value, result.content[0].name)

    def test_search_by_name_not_found_name_01(self):
        search_type = "name"
        search_value = "xxxyyyyzzz"
        result = self.search_manager.search_by_type(search_type, search_value)
        self.assertIsNotNone(result)
        self.assertIsNotNone(result.content)
        self.assertIsNotNone(result.message)
        self.assertEqual(0, len(result.content))

    def test_search_by_inchi_success_01(self):
        search_type = "inchi"
        search_value = "NLDDIKRKFXEWBK-AWEZNQCLSA-N"
        expected_value = "gingerol"
        result = self.search_manager.search_by_type(search_type, search_value)
        self.assertIsNotNone(result)
        self.assertIsNotNone(result.content)
        self.assertEqual(1, len(result.content))
        self.assertEqual(expected_value, result.content[0].name)

    def test_search_by_inchi_success_02(self):
        search_type = "inchi"
        search_value = "InChI=1S/C17H26O4/c1-3-4-5-6-14(18)12-15(19)9-7-13-8-10-16(20)17(11-13)21-2/h8,10-11,14,18,20H,3-7,9,12H2,1-2H3/t14-/m0/s1"
        expected_value = "CHEBI:10136"
        result = self.search_manager.search_by_type(search_type, search_value)
        self.assertIsNotNone(result)
        self.assertIsNotNone(result.content)
        self.assertEqual(1, len(result.content))
        self.assertEqual(expected_value, result.content[0].databaseId)

    def test_search_by_inchi_success_03(self):
        search_type = "inchi"
        search_value = "1S/C17H26O4/c1-3-4-5-6-14(18)12-15(19)9-7-13-8-10-16(20)17(11-13)21-2/h8,10-11,14,18,20H,3-7,9,12H2,1-2H3/t14-/m0/s1"
        expected_value = "CHEBI:10136"
        result = self.search_manager.search_by_type(search_type, search_value)
        self.assertIsNotNone(result)
        self.assertIsNotNone(result.content)
        self.assertEqual(1, len(result.content))
        self.assertEqual(expected_value, result.content[0].databaseId)

    def test_search_by_inchi_fail_01(self):
        search_type = "inchi"
        search_value = "XX1S/C17H26O4/c1-3-4-5-6-14"
        result = self.search_manager.search_by_type(search_type, search_value)
        self.assertIsNotNone(result)
        self.assertIsNotNone(result.content)
        self.assertIsNotNone(result.message)
        self.assertEqual(0, len(result.content))

    def test_search_by_smiles_success_01(self):
        search_type = "smiles"
        search_value = "O[C@H](CCCCC)CC(=O)CCC1=CC(OC)=C(O)C=C1"
        expected_value = "CHEBI:181480"
        result = self.search_manager.search_by_type(search_type, search_value)
        self.assertIsNotNone(result)
        self.assertIsNotNone(result.content)
        self.assertEqual(1, len(result.content))
        self.assertEqual(expected_value, result.content[0].databaseId)

    def test_search_by_smiles_success_02(self):
        search_type = "smiles"
        search_value = "CO"
        expected_value = "CHEBI:17790"
        result = self.search_manager.search_by_type(search_type, search_value)
        self.assertIsNotNone(result)
        self.assertIsNotNone(result.content)
        self.assertEqual(1, len(result.content))
        self.assertEqual(expected_value, result.content[0].databaseId)

    def test_search_by_smiles_not_found_01(self):
        search_type = "smiles"
        search_value = "XO[C@H](CCCCC)CC(=O)CCC1=CC(OC)=C(O)C=C1"
        result = self.search_manager.search_by_type(search_type, search_value)
        self.assertIsNotNone(result)
        self.assertIsNotNone(result.content)
        self.assertIsNotNone(result.message)
        self.assertEqual(0, len(result.content))
