import os
import json
import pytz

from dlake import Datalake
from datetime import datetime

datalake = Datalake(os.getenv('PRAVAH_DB_USER'), os.getenv('PRAVAH_DB_PASSWORD'), '/SolarPowerProduction')

def todays():
    today_string = datetime.now().strftime('%Y/%m/%d')
    cur = datalake.aggregate(
        match={'geospace': '/in/delhi'},
        start=today_string + ' 00:00:00',
        end=today_string + ' 23:59:59',
        pipeline = [
            {
                '$unwind': '$item.stations'
            }, {
                '$group': {
                    '_id': '$item.stations.id',
                    'powerGeneratedToday': {
                        '$push': {
                            'timestamp': '$item.stations.timestamp',
                            'powerGeneratedToday': '$item.stations.powerGenerationParameters.powerGeneratedToday'
                        }
                    },
                    'currentPowerOutput': {
                        '$push': {
                            'timestamp': '$item.stations.timestamp',
                            'currentPowerOutput': '$item.stations.powerGenerationParameters.currentPowerOutput'
                        }
                    },
                    'irradiation': {
                        '$push': {
                            'timestamp': '$item.stations.timestamp',
                            'irradiation': '$item.stations.powerGenerationParameters.irradiation'
                        }
                    },
                    'name': {
                        '$first': '$item.stations.info.name'
                    }
                }
            }
        ]
    )
    return list(cur)

""" def update():
    today_string = datetime.now().strftime('%Y/%m/%d')
    cur = datalake.aggregate(
        match={'geospace': '/in/delhi'},
        start=today_string + ' 00:00:00',
        end=today_string + ' 23:59:59',
        pipeline = [
            {
                '$unwind': '$item.stations'
            }, {
                '$match': {
                    'item.stations.powerGenerationParameters.currentPowerOutput': {
                        '$gte': 500
                    }
                }
            }, {
                '$group': {
                    '_id': '$_id'
                }
            }
        ]
    )
    for ik in cur:
        o = datalake.collection.find_one({'_id': ik['_id']})
        for s in o['item']['stations']:
            if s['powerGenerationParameters']['currentPowerOutput'] > 500:
                s['powerGenerationParameters']['currentPowerOutput'] /= 1000.0
            for i in s['inverterList']:
                if i['powerGenerationParameters']['currentPowerOutput'] > 500:
                    i['powerGenerationParameters']['currentPowerOutput'] /= 1000.0
        print(o)
        print()
        datalake.collection.update(
            {'_id': ik['_id']},
            o
        )
"""

def agg():
    cur = datalake.aggregate(
        match={'geospace': '/in/delhi'},
        past_hours=24*7*20,
        pipeline = [
            {
                '$addFields': {
                    'date': {
                        '$dateToString': {
                            'format': '%Y-%m-%d',
                            'date': {'$toDate': '$_id' }
                        }
                    }
                }
            }, {
                '$unwind': '$item.stations'
            }, {
                '$group': {
                    '_id': {
                        'date': '$date',
                        'id': '$item.stations.id'
                    },
                    'powerGeneratedTodayMax': {
                        '$max': '$item.stations.powerGenerationParameters.powerGeneratedToday'
                    }
                }
            }, {
                '$addFields': {
                    'actualDate': {
                        '$toDate': '$_id.date'
                    }
                }
            }, {
                '$group': {
                    '_id': '$actualDate',
                    'powerGeneratedTodayMaxSum': {
                        '$sum': '$powerGeneratedTodayMax'
                    }
                }
            }, {
                '$sort': {
                    '_id': 1
                }
            }, 
            
        ]
    )
    return list(cur)

def processData(cur):
    stations = {}
    for i in cur:
        print(i)
        continue
        try:
            for s in i['item']['stations']:
                try:
                    stations[s['id']]
                except:
                    stations[s['id']] = {
                        'currentPowerOutput': {
                            'timestamp': [],
                            'val': []
                        },
                        'powerGeneratedToday': {
                            'timestamp': [],
                            'val': []
                        },
                        'irradiation': {
                            'timestamp': [],
                            'val': []
                        }
                    }
                try:
                    stations[s['id']]['currentPowerOutput']['val'].append(s['powerGenerationParameters']['currentPowerOutput'])
                    stations[s['id']]['currentPowerOutput']['timestamp'].append(s['timestamp'])
                except:
                    pass
                try:
                    stations[s['id']]['powerGeneratedToday']['val'].append(s['powerGenerationParameters']['powerGeneratedToday'])
                    stations[s['id']]['powerGeneratedToday']['timestamp'].append(s['timestamp'])
                except:
                    pass
                try:
                    stations[s['id']]['irradiation']['val'].append(s['powerGenerationParameters']['irradiation'])
                    stations[s['id']]['irradiation']['timestamp'].append(s['timestamp'])
                except:
                    pass
        except:
            continue
    print(stations)

def fetch_capacity():
    cur = datalake.aggregate(
        match={'geospace': '/in/delhi'},
        past_hours=240000,
        pipeline = [
            {
                '$unwind': '$item.stations'
            }, {
                '$group': {
                    '_id': '$item.stations.id',
                    'info': {
                        '$first': '$item.stations.info'
                    }
                }
            }
        ]
    )
    
    l = {
        'data': list(cur)
    }
    print(l)
    with open('station_static.json', 'w') as sf:
        json.dump(l, sf)

#fetch_capacity()