vals = {
    'yes': 0,
    'residential': 1,
    'service': 2,
    'unclassified': 3,
    'stream': 4,
    'track': 5,
    'water': 6,
    'footway': 7,
    'tertiary': 8,
    'private': 9,
    'tree': 10,
    'path': 11,
    'forest': 12,
    'secondary': 13,
    'house': 14,
    'no': 15,
    'asphalt': 16,
    'wood': 17,
    'grass': 18,
    'paved': 19,
    'primary': 20,
    'unpaved': 21,
    'bus_stop': 22,
    'parking': 23,
    'parking_aisle': 24,
    'rail': 25,
    'driveway': 26,
    '8': 27,
    'administrative': 28,
    'locality': 29,
    'turning_circle': 30,
    'crossing': 31,
    'village': 32,
    'fence': 33,
    'grade2': 34,
    'coastline': 35,
    'grade3': 36,
    'farmland': 37,
    'hamlet': 38,
    'hut': 39,
    'meadow': 40,
    'wetland': 41,
    'cycleway': 42,
    'river': 43,
    'school': 44,
    'trunk': 45,
    'gravel': 46,
    'place_of_worship': 47,
    'farm': 48,
    'grade1': 49,
    'traffic_signals': 50,
    'wall': 51,
    'garage': 52,
    'gate': 53,
    'motorway': 54,
    'living_street': 55,
    'pitch': 56,
    'grade4': 57,
    'industrial': 58,
    'road': 59,
    'ground': 60,
    'scrub': 61,
    'motorway_link': 62,
    'steps': 63,
    'ditch': 64,
    'swimming_pool': 65,
    'grade5': 66,
    'park': 67,
    'apartments': 68,
    'restaurant': 69,
    'designated': 70,
    'bench': 71,
    'survey_point': 72,
    'pedestrian': 73,
    'hedge': 74,
    'reservoir': 75,
    'riverbank': 76,
    'alley': 77,
    'farmyard': 78,
    'peak': 79,
    'level_crossing': 80,
    'roof': 81,
    'dirt': 82,
    'drain': 83,
    'garages': 84,
    'entrance': 85,
    'street_lamp': 86,
    'deciduous': 87,
    'fuel': 88,
    'trunk_link': 89,
    'information': 90,
    'playground': 91,
    'supermarket': 92,
    'primary_link': 93,
    'concrete': 94,
    'mixed': 95,
    'permissive': 96,
    'orchard': 97,
    'grave_yard': 98,
    'canal': 99,
    'garden': 100,
    'spur': 101,
    'paving_stones': 102,
    'rock': 103,
    'bollard': 104,
    'convenience': 105,
    'cemetery': 106,
    'post_box': 107,
    'commercial': 108,
    'pier': 109,
    'bank': 110,
    'hotel': 111,
    'cliff': 112,
    'retail': 113,
    'construction': 114,
    '-1': 115,
    'fast_food': 116,
    'coniferous': 117,
    'cafe': 118,
    '6': 119,
    'kindergarten': 120,
    'tower': 121,
    'hospital': 122,
    'yard': 123,
    'sand': 124,
    'public_building': 125,
    'cobblestone': 126,
    'destination': 127,
    'island': 128,
    'abandoned': 129,
    'vineyard': 130,
    'recycling': 131,
    'agricultural': 132,
    'isolated_dwelling': 133,
    'pharmacy': 134,
    'post_office': 135,
    'motorway_junction': 136,
    'pub': 137,
    'allotments': 138,
    'dam': 139,
    'secondary_link': 140,
    'lift_gate': 141,
    'siding': 142,
    'stop': 143,
    'main': 144,
    'farm_auxiliary': 145,
    'quarry': 146,
    '10': 147,
    'station': 148,
    'platform': 149,
    'taxiway': 150,
    'limited': 151,
    'sports_centre': 152,
    'cutline': 153,
    'detached': 154,
    'storage_tank': 155,
    'basin': 156,
    'bicycle_parking': 157,
    'telephone': 158,
    'terrace': 159,
    'town': 160,
    'suburb': 161,
    'bus': 162,
    'compacted': 163,
    'toilets': 164,
    'heath': 165,
    'works': 166,
    'tram': 167,
    'beach': 168,
    'culvert': 169,
    'fire_station': 170,
    'recreation_ground': 171,
    'bakery': 172,
    'police': 173,
    'atm': 174,
    'clothes': 175,
    'tertiary_link': 176,
    'waste_basket': 177,
    'attraction': 178,
    'viewpoint': 179,
    'bicycle': 180,
    'church': 181,
    'shelter': 182,
    'drinking_water': 183,
    'marsh': 184,
    'picnic_site': 185,
    'hairdresser': 186,
    'bridleway': 187,
    'retaining_wall': 188,
    'buffer_stop': 189,
    'nature_reserve': 190,
    'village_green': 191,
    'university': 192,
    '1': 193,
    'bar': 194,
    'townhall': 195,
    'mini_roundabout': 196,
    'camp_site': 197,
    'aerodrome': 198,
    'stile': 199,
    '9': 200,
    'car_repair': 201,
    'parking_space': 202,
    'library': 203,
    'pipeline': 204,
    'true': 205,
    'cycle_barrier': 206,
    '4': 207,
    'museum': 208,
    'spring': 209,
    'hunting_stand': 210,
    'disused': 211,
    'car': 212,
    'tram_stop': 213,
    'land': 214,
    'fountain': 215,
    'hiking': 216,
    'manufacture': 217,
    'vending_machine': 218,
    'kiosk': 219,
    'swamp': 220,
    'unknown': 221,
    '7': 222,
    'islet': 223,
    'shed': 224,
    'switch': 225,
    'rapids': 226,
    'office': 227,
    'bay': 228,
    'proposed': 229,
    'common': 230,
    'weir': 231,
    'grassland': 232,
    'customers': 233,
    'social_facility': 234,
    'hangar': 235,
    'doctors': 236,
    'stadium': 237,
    'give_way': 238,
    'greenhouse': 239,
    'guest_house': 240,
    'viaduct': 241,
    'doityourself': 242,
    'runway': 243,
    'bus_station': 244,
    'water_tower': 245,
    'golf_course': 246,
    'conservation': 247,
    'block': 248,
    'college': 249,
    'wastewater_plant': 250,
    'subway': 251,
    'halt': 252,
    'forestry': 253,
    'florist': 254,
    'butcher': 255}


def getValues():
    return vals
