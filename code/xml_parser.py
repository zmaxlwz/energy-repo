import xml.etree.ElementTree as ET

class XML_Parser:
    def __init__(self):
        """ initialize variables

        """
        #namespace for the XML
        self.ns = {'citytouch_ns': 'http://schemas.citytouch.com/Common/v1.0',
                   'instance_ns': 'http://www.w3.org/2001/XMLSchema-instance'}

    def parse_from_file(self, file_path):
        """ parse XML from a file

        """
        tree = ET.parse(file_path)
        root = tree.getroot()
        self.parser(root)

    def parse_from_string(self, xml_string):
        """ parse XML from a string

        """    
        root = ET.fromstring(xml_string)
        self.parser(root)

    def parser(self, root):
        """ parse the XML tree from the input root

        """  
        #print(root.tag)
        #print(root.attrib)

        #for child in root:
        #  print(child.tag, child.attrib)

        shapes = root.find('citytouch_ns:shapes', self.ns)
        #print(shapes.tag)
        for dimming_shape in shapes:
            #print(dimming_shape.tag, dimming_shape.attrib)    
            shape_id = dimming_shape.find('citytouch_ns:Id', self.ns).text
            shape_is_default = dimming_shape.find('citytouch_ns:IsDefault', self.ns).text
            shape_mode = dimming_shape.find('citytouch_ns:ShapeMode', self.ns).text
            shape_sunrise_offset = dimming_shape.find('citytouch_ns:SunriseOffset', self.ns).text
            shape_runset_offset = dimming_shape.find('citytouch_ns:SunsetOffset', self.ns).text
            shape_name = dimming_shape.find('citytouch_ns:Name', self.ns).text
            shape_color_argb = dimming_shape.find('citytouch_ns:ColorArgb', self.ns).text
            print(shape_id, shape_is_default, shape_mode, shape_sunrise_offset, shape_runset_offset, shape_name, shape_color_argb)
            
            shape_items = dimming_shape.find('citytouch_ns:Items', self.ns)
            for dimming_shape_item in shape_items:
                item_id = dimming_shape_item.find('citytouch_ns:Id', self.ns).text
                item_minutes = dimming_shape_item.find('citytouch_ns:Minutes', self.ns).text
                item_percent = dimming_shape_item.find('citytouch_ns:Percent', self.ns).text
                item_time_mode = dimming_shape_item.find('citytouch_ns:TimeMode', self.ns).text
                print("--> ", item_id, item_minutes, item_percent, item_time_mode)

  
