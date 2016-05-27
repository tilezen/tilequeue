
################################################################################
# Copyright (c) QinetiQ Plc 2003
#
# Licensed under the LGPL. For full license details see the LICENSE file.
################################################################################

"""
A parser for the Well Text Binary format of OpenGIS types.
"""
#
# 2.5d spec: http://gdal.velocet.ca/projects/opengis/twohalfdsf.html
#

import sys, traceback, struct


# based on xdrlib.Unpacker
class _ExtendedUnPacker:
    """
    A simple binary struct parser, only implements the types that are need for the WKB format.
    """
    
    def __init__(self,data):
        self.reset(data)
        self.setEndianness('XDR')

    def reset(self, data):
        self.__buf = data
        self.__pos = 0

    def get_position(self):
        return self.__pos

    def set_position(self, position):
        self.__pos = position

    def get_buffer(self):
        return self.__buf

    def done(self):
        if self.__pos < len(self.__buf):
            raise ExceptionWKBParser('unextracted data remains')

    def setEndianness(self,endianness):
        if endianness == 'XDR':
            self._endflag = '>'
        elif endianness == 'NDR':
            self._endflag = '<'
        else:
            raise ExceptionWKBParser('Attempt to set unknown endianness in ExtendedUnPacker')

    def unpack_byte(self):
        i = self.__pos
        self.__pos = j = i+1
        data = self.__buf[i:j]
        if len(data) < 1:
            raise EOFError
        byte = struct.unpack('%sB' % self._endflag, data)[0]
        return byte

    def unpack_uint32(self):
        i = self.__pos
        self.__pos = j = i+4
        data = self.__buf[i:j]
        if len(data) < 4:
            raise EOFError
        uint32 = struct.unpack('%si' % self._endflag, data)[0]
        return uint32

    def unpack_short(self):
        i = self.__pos
        self.__pos = j = i+2
        data = self.__buf[i:j]
        if len(data) < 2:
            raise EOFError
        short = struct.unpack('%sH' % self._endflag, data)[0]
        return short

    def unpack_double(self):
        i = self.__pos
        self.__pos = j = i+8
        data = self.__buf[i:j]
        if len(data) < 8:
            raise EOFError
        return struct.unpack('%sd' % self._endflag, data)[0]

class ExceptionWKBParser(Exception):
    '''This is the WKB Parser Exception class.'''
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return `self.value`
    
class GeomEncoder:

    _count = 0

    def __init__(self, tileSize):
        """
        Initialise a new WKBParser.

        """
        
        self._typemap = {1: self.parsePoint,
                         2: self.parseLineString,
                         3: self.parsePolygon,
                         4: self.parseMultiPoint,
                         5: self.parseMultiLineString,
                         6: self.parseMultiPolygon,
                         7: self.parseGeometryCollection}
        self.coordinates = []
        self.index = []
        self.position = 0
        self.lastX = 0
        self.lastY = 0
        self.dropped = 0
        self.num_points = 0
        self.isPoint = True
        self.tileSize = tileSize - 1
        self.first = True

    def parseGeometry(self, geometry):
        

        """
        A factory method for creating objects of the correct OpenGIS type.
        """

        self.coordinates = []
        self.index = []
        self.position = 0
        self.lastX = 0 
        self.lastY = 0
        self.isPoly = False
        self.isPoint = True;
        self.dropped = 0;
        self.first = True
        # Used for exception strings
        self._current_string = geometry
        
        reader = _ExtendedUnPacker(geometry)
                
        # Start the parsing
        self._dispatchNextType(reader)
        

    def _dispatchNextType(self,reader):
        """
        Read a type id from the binary stream (reader) and call the correct method to parse it.
        """
        
        # Need to check endianess here!
        endianness = reader.unpack_byte()
        if endianness == 0:
            reader.setEndianness('XDR')
        elif endianness == 1:
            reader.setEndianness('NDR')
        else:
            raise ExceptionWKBParser("Invalid endianness in WKB format.\n"\
                                     "The parser can only cope with XDR/big endian WKB format.\n"\
                                     "To force the WKB format to be in XDR use AsBinary(<fieldname>,'XDR'")
            
        
        geotype = reader.unpack_uint32() 

        mask = geotype & 0x80000000 # This is used to mask of the dimension flag.

        srid = geotype & 0x20000000
        # ignore srid ...
        if srid != 0:
            reader.unpack_uint32()

        dimensions = 2
        if mask == 0:
            dimensions = 2
        else:
            dimensions = 3
       
        geotype = geotype & 0x1FFFFFFF
        # Despatch to a method on the type id.
        if self._typemap.has_key(geotype):
            self._typemap[geotype](reader, dimensions)
        else:
            raise ExceptionWKBParser('Error type to dispatch with geotype = %s \n'\
                                     'Invalid geometry in WKB string: %s' % (str(geotype),
                                                                             str(self._current_string),))
        
    def parseGeometryCollection(self, reader, dimension):
        try:
            num_geoms = reader.unpack_uint32()

            for _ in xrange(0,num_geoms):
                self._dispatchNextType(reader)

        except:
            _, value, tb = sys.exc_info()[:3]
            error = ("%s , %s \n" % (type, value))
            for bits in traceback.format_exception(type,value,tb):
                error = error + bits + '\n'
            del tb
            raise ExceptionWKBParser("Caught unhandled exception parsing GeometryCollection: %s \n"\
                                     "Traceback: %s\n" % (str(self._current_string),error))


    def parseLineString(self, reader, dimensions):
        self.isPoint = False;
        try:
            num_points = reader.unpack_uint32()

            self.num_points = 0;
            
            for _ in xrange(0,num_points):
                self.parsePoint(reader,dimensions)
                
            self.index.append(self.num_points)
            #self.lastX = 0 
            #self.lastY = 0
            self.first = True

        except:
            _, value, tb = sys.exc_info()[:3]
            error = ("%s , %s \n" % (type, value))
            for bits in traceback.format_exception(type,value,tb):
                error = error + bits + '\n'
            del tb
            print error
            raise ExceptionWKBParser("Caught unhandled exception parsing Linestring: %s \n"\
                                     "Traceback: %s\n" % (str(self._current_string),error))

    
    def parseMultiLineString(self, reader, dimensions):
        try:
            num_linestrings = reader.unpack_uint32()

            for _ in xrange(0,num_linestrings):
                self._dispatchNextType(reader)

        except:
            _, value, tb = sys.exc_info()[:3]
            error = ("%s , %s \n" % (type, value))
            for bits in traceback.format_exception(type,value,tb):
                error = error + bits + '\n'
            del tb
            raise ExceptionWKBParser("Caught unhandled exception parsing MultiLineString: %s \n"\
                                     "Traceback: %s\n" % (str(self._current_string),error))
        
    
    def parseMultiPoint(self, reader, dimensions):
        try:
            num_points = reader.unpack_uint32()

            for _ in xrange(0,num_points):
                self._dispatchNextType(reader)
        except:
            _, value, tb = sys.exc_info()[:3]
            error = ("%s , %s \n" % (type, value))
            for bits in traceback.format_exception(type,value,tb):
                error = error + bits + '\n'
            del tb
            raise ExceptionWKBParser("Caught unhandled exception parsing MultiPoint: %s \n"\
                                     "Traceback: %s\n" % (str(self._current_string),error))

    
    def parseMultiPolygon(self, reader, dimensions):
        try:
            num_polygons = reader.unpack_uint32()
            for n in xrange(0,num_polygons):
                if n > 0:
                    self.index.append(0);
                  
                self._dispatchNextType(reader)
        except:
            _, value, tb = sys.exc_info()[:3]
            error = ("%s , %s \n" % (type, value))
            for bits in traceback.format_exception(type,value,tb):
                error = error + bits + '\n'
            del tb
            raise ExceptionWKBParser("Caught unhandled exception parsing MultiPolygon: %s \n"\
                                     "Traceback: %s\n" % (str(self._current_string),error))

        
    def parsePoint(self, reader, dimensions):
        x = reader.unpack_double()
        y = reader.unpack_double()
      
        if dimensions == 3:
            reader.unpack_double()

        xx = int(round(x))
        # flip upside down
        yy = self.tileSize - int(round(y))
        
        if self.first or xx - self.lastX != 0 or yy - self.lastY != 0:
            self.coordinates.append(xx - self.lastX)
            self.coordinates.append(yy - self.lastY)
            self.num_points += 1
        else:
            self.dropped += 1;
        
        self.first = False
        self.lastX = xx
        self.lastY = yy
       

    def parsePolygon(self, reader, dimensions):
        self.isPoint = False;
        try:
            num_rings = reader.unpack_uint32()

            for _ in xrange(0,num_rings):
                self.parseLinearRing(reader,dimensions)
            
            self.isPoly = True
            
        except:
            _, value, tb = sys.exc_info()[:3]
            error = ("%s , %s \n" % (type, value))
            for bits in traceback.format_exception(type,value,tb):
                error = error + bits + '\n'
            del tb
            raise ExceptionWKBParser("Caught unhandled exception parsing Polygon: %s \n"\
                                     "Traceback: %s\n" % (str(self._current_string),error))

    def parseLinearRing(self, reader, dimensions):
        self.isPoint = False;
        try:
            num_points = reader.unpack_uint32()
            
            self.num_points = 0;
            
            # skip the last point
            for _ in xrange(0,num_points-1):
                self.parsePoint(reader,dimensions)

            # skip the last point                
            reader.unpack_double()
            reader.unpack_double()
            if dimensions == 3:
                reader.unpack_double()
                
            self.index.append(self.num_points)
    
            self.first = True
            
        except:
            _, value, tb = sys.exc_info()[:3]
            error = ("%s , %s \n" % (type, value))
            for bits in traceback.format_exception(type,value,tb):
                error = error + bits + '\n'
            del tb
            raise ExceptionWKBParser("Caught unhandled exception parsing LinearRing: %s \n"\
                                     "Traceback: %s\n" % (str(self._current_string),error))