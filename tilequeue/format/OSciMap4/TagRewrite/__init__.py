import logging

# TODO test the lua osm2pgsql for preprocessing !
#
# fix tags from looking things up in wiki where a value should be used with a specific key,
# i.e. one combination has a wiki page and more use in taginfo and the other does not
# TODO add:
# natural=>meadow
# landuse=>greenhouse,public,scrub
# aeroway=>aerobridge
# leisure=>natural_reserve

def fixTag(tag):
    drop = False
     
    if tag[1] is None:
        drop = True
    
    key = tag[0].lower();
    
    if key == 'highway':
        # FIXME remove ; separated part of tags
        return (key, tag[1].lower().split(';')[0])
     
    # fixed in osm   
    #if key == 'leisure':
    #    value = tag[1].lower();
    #    if value in ('village_green', 'recreation_ground'):
    #        return ('landuse', value)
    #    else:
    #        return (key, value)
            
    elif key == 'natural':
        value = tag[1].lower();
        #if zoomlevel <= 9 and not value in ('water', 'wood'):
        #    return None
        
        if value in ('village_green', 'meadow'):
            return ('landuse', value)
        if value == 'mountain_range':
            drop = True
        else:
            return (key, value)
            
    elif key == 'landuse':
        value = tag[1].lower();
        #if zoomlevel <= 9 and not value in ('forest', 'military'):
        #    return None
        
        # strange for natural_reserve: more common this way round...
        if value in ('park', 'natural_reserve'):
            return ('leisure', value)
        elif value == 'field':
            # wiki: Although this landuse is rendered by Mapnik, it is not an officially 
            # recognised OpenStreetMap tag. Please use landuse=farmland instead.
            return (key, 'farmland')
        elif value in ('grassland', 'scrub'):
            return ('natural', value)
        else:
            return (key, value)
    
    elif key == 'oneway':
        value = tag[1].lower();
        if value in ('yes', '1', 'true'):
            return (key, 'yes')
        else: 
            drop = True
    
    elif key == 'area':
        value = tag[1].lower();
        if value in ('yes', '1', 'true'):
            return (key, 'yes')
        # might be used to indicate that a closed way is not an area
        elif value in ('no'):
            return (key, 'no')
        else: 
            drop = True
            
    elif key == 'bridge':
        value = tag[1].lower();
        if value in ('yes', '1', 'true'):
            return (key, 'yes')
        elif value in ('no', '-1', '0', 'false'):
            drop = True
        else:
            return (key, value)
        
    elif key == 'tunnel':
        value = tag[1].lower();
        if value in ('yes', '1', 'true'):
            return (key, 'yes')
        elif value in ('no', '-1', '0', 'false'):
            drop = True
        else:
            return (key, value)
        
    elif key == 'water':
        value = tag[1].lower();
        if value in ('lake;pond'):
            return (key, 'pond')
        else:
            return (key, value)
        
    if drop:
        logging.debug('drop tag: %s %s' % (tag[0], tag[1]))
        return None
    
    return tag

