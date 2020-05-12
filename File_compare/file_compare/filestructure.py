from lxml import etree


class CompareRoot:
    __type = ""
    __length = 0
    __content_name_position = 0
    __formats = dict()

    def __init__(self, type, length, content_name_position, formats=list()):
        # type: (str,int,int,list) -> CompareRoot
        self.__type = type
        self.__length = length
        self.__content_name_position = content_name_position

        for f in formats:
            self.__formats[f.name] = f

    @property
    def type(self):
        # type: () -> str
        return self.__type

    @property
    def length(self):
        # type: () -> int
        return self.__length

    @property
    def content_name_position(self):
        # type: () -> int
        return self.__content_name_position

    def format(self, key):
        # type: (str) -> RecordFormat
        return self.__formats.get(key)

    @staticmethod
    def __get_root_from_xml(file):
        # type: (str) -> CompareRoot

        xml_text = "<file/>"
        with open(file, "rt") as xml:
            xml_text = xml.read()

        schema = etree.XMLSchema(file="resources/compare.xsd")

        parser = etree.XMLParser(schema=schema)
        root = etree.XML(xml_text, parser=parser)

        formats = root.findall("./formats/format")
        rfs = list()
        for format in formats:

            contents = format.findall("./contents/content")

            rcs = list()
            for content in contents:

                elements = content.findall("./elements/element")

                ces = list()
                for element in elements:
                    ce = CompareElement(element.get("name"), int(element.get("position")), int(element.get("length")),
                                        element.get("oldvalue"), element.get("newvalue"), element.get("comment"),
                                        bool(element.get("agent")), bool(element.get("airline")),
                                        bool(element.get("docnum")),
                                        bool(element.get("csidate")), element.get("desc"), element.get("commentdesc"))
                    ces.append(ce)

                rc = RecordContent(content.get("name"), bool(content.get("checkcanx")), content.get("elementname"),
                                   content.get("comment"), content.get("commentdesc"), bool(content.get("group")),
                                   bool(content.get("groupend")), ces)
                rcs.append(rc)

            rf = RecordFormat(format.get("name"), rcs)
            rfs.append(rf)

        cr = CompareRoot(str(root.get("type")), int(root.get("length")), int(root.get("contentnameposition")), rfs)
        return cr

    @staticmethod
    def get_hot_root():
        return CompareRoot.__get_root_from_xml("resources/comparehot.xml")

    @staticmethod
    def get_csi_root():
        return CompareRoot.__get_root_from_xml("resources/comparecsi.xml")


class RecordFormat:
    __name = ""
    __contents = dict()

    def __init__(self, name, contents=list()):
        self.__name = name

        for c in contents:
            self.__contents[c.name] = c

    @property
    def name(self):
        # type: () -> str
        return self.__name

    def content(self, key):
        # type: (str) -> RecordContent
        return self.__contents.get(key)


class RecordContent:
    __name = ""
    __check_canx = False
    __element_name = ""
    __comment = ""
    __comment_desc = ""
    __group = False
    __group_end = False
    __element_map = dict()

    def __init__(self, name, check_canx=False, element_name="", comment="", comment_desc="", group=False,
                 group_end=False, elements=dict()):
        # type: (str,bool,str,str,str,bool,bool,list) -> RecordContent
        self.__name = name
        self.__check_canx = check_canx
        self.__element_name = element_name
        self.__comment = comment
        self.__comment_desc = comment_desc
        self.__group = group
        self.__group_end = group_end

        for e in elements:
            self.__element_map[e.name] = e

    @property
    def name(self):
        # type: () -> str
        return self.__name

    @property
    def check_canx(self):
        # type: () -> bool
        return self.__check_canx

    @property
    def element_name(self):
        # type: () -> str
        return self.__element_name

    @property
    def comment(self):
        # type: () -> str
        return self.__comment

    @property
    def comment_desc(self):
        # type: () -> str
        return self.__comment_desc

    @property
    def group(self):
        # type: () -> bool
        return self.__group

    @property
    def group_end(self):
        # type: () -> bool
        return self.__group_end

    def element(self, key):
        # type: (str) -> CompareElement
        return self.__element_map.get(key)

    def elements(self):
        # type: () -> list
        return self.__element_map.keys()


class CompareElement:
    __name = ""
    __position = 0
    __length = 0
    __desc = ""
    __old_value = ""
    __new_value = ""
    __comment = ""
    __comment_desc = ""
    __doc_num = False
    __airline = False
    __agent = False
    __csi_date = False

    def __init__(self, name, position, length, old_value="", new_value="", comment="", agent=False, airline=False,
                 doc_num=False, csi_date=False, desc="", comment_desc=""):
        # type: (str,int,int,str,str,str,bool,bool,bool,bool,str,str) -> CompareElement
        self.__name = name
        self.__position = position
        self.__length = length
        self.__old_value = old_value
        self.__new_value = new_value
        self.__comment = comment
        self.__agent = agent
        self.__airline = airline
        self.__doc_num = doc_num
        self.__csi_date = csi_date
        self.__desc = desc
        self.__comment_desc = comment_desc

    @property
    def name(self):
        # type: () -> str
        return self.__name

    @property
    def position(self):
        # type: () -> int
        return self.__position

    @property
    def length(self):
        # type: () -> int
        return self.__length

    @property
    def desc(self):
        # type: () -> str
        return self.__desc

    @property
    def old_value(self):
        # type: () -> str
        return self.__old_value

    @property
    def new_value(self):
        # type: () -> str
        return self.__new_value

    @property
    def comment(self):
        # type: () -> str
        return self.__comment

    @property
    def comment_desc(self):
        # type: () -> str
        return self.__comment_desc

    @property
    def doc_num(self):
        # type: () -> bool
        return self.__doc_num

    @property
    def airline(self):
        # type: () -> bool
        return self.__airline

    @property
    def agent(self):
        # type: () -> bool
        return self.__agent

    @property
    def csi_date(self):
        # type: () -> bool
        return self.__csi_date


class CompareRecord:
    __record_line = None
    __content = None
    __element_map = dict()

    def __init__(self, record_line, content):
        # type: (str, RecordContent) -> CompareRecord

        self.__record_line = record_line
        self.__content = content

    @property
    def content(self):
        # type: () -> RecordContent
        return self.__content

    @property
    def element_map(self):
        # type: () -> dict
        return self.__element_map

    @property
    def record_line(self):
        # type: () -> str
        return self.__record_line

    def element(self, key):
        # type: (str) -> str

        result = self.element_map.get(key)
        if result is None:
            element = self.content.element(key)
            result = self.record_line[element.position - 1: element.position - 1 + element.length]
            self.element_map[key] = result
        return result

    def comment(self, key):
        # type: (str) -> str
        return self.content.element(key).comment

    def comment_desc(self, key):
        # type: (str) -> str
        return self.content.element(key).comment_desc

    def new_value(self, key):
        # type: (str) -> str
        return self.content.element(key).new_value

    def old_value(self, key):
        # type: (str) -> str
        return self.content.element(key).old_value

    def agent(self, key):
        # type: (str) -> bool
        return self.content.element(key).agent

    def airline(self, key):
        # type: (str) -> bool
        return self.content.element(key).airline

    def check_length(self, key):
        # type: (str) -> bool
        return self.content.element(key).check_length

    def csi_date(self, key):
        # type: (str) -> bool
        return self.content.element(key).csi_date

    def doc_num(self, key):
        # type: (str) -> bool
        return self.content.element(key).doc_num

    def group(self):
        # type: () -> bool
        return self.content.group

    def group_end(self):
        # type: () -> bool
        return self.content.group_end

    def parse_record(self):
        # type: () -> None
        for e in self.content.elements():
            element = self.content.element(e)
            result = self.record_line[element.position - 1: element.position - 1 + element.length]
            self.element_map[e] = result
