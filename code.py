import requests
import pymongo
import json, xmltodict
import time
from kafka import KafkaProducer


# XML TO JSON
def convert_to_json(txt):
  return xmltodict.parse(txt)


# MONGODB
# create a connection
client = pymongo.MongoClient(host = 'localhost', port = 27000)

# create/or connect to a database
db = client.bahn

# create/use a collection
bhf = db.bhf
timetable = db.timetable
changes = db.changes


# * KAFKA #################################

# KAFKA PRODUCER

producer = KafkaProducer(bootstrap_servers = "172.18.0.3:9092", #kafka-server wenn in Docker 
                         value_serializer = lambda m: json.dumps(m).encode('utf-8')
                         )

producer.send(topic = "topic1", value = {"student": "Stefan"})


#! test: consumer. Problem: Er hängt beim Auslesen
from kafka import KafkaConsumer

consumer = KafkaConsumer("topic1",
                         bootstrap_servers = "localhost:9092",
                         auto_offset_reset = 'earliest',
                         #enable_auto_commit = True,
                         value_deserializer = lambda m: json.loads(m.decode('utf-8')),
                         group_id = 'my-group-1')

for message in consumer:
    print (message.value)

#!#############


#######################################################################

# https://developer.deutschebahn.com/store/apis/info?name=Timetables&version=v1&provider=DBOpenData&#/

# BAHNHÖFE

station_name = "münchen-pasing"

url = f"https://api.deutschebahn.com/timetables/v1/station/{station_name}"
headers = {"Authorization": "Bearer c2717c0f768243e30011b8b3104f6d3d",
           "Accept": "application/xml"}

r = requests.get(url, headers = headers)

bahnhof_info = convert_to_json(r.text)

# insert into db
db.bhf.insert(bahnhof_info["stations"]["station"])


# TIMETABLE

# Triggern: am Beginn jeder Stunde für die darauf folgende Stunde

eva = 8004158
date = 210508
hour = 21

url = f"https://api.deutschebahn.com/timetables/v1/plan/{eva}/{date}/{hour}"
r = requests.get(url, headers = headers)

if r.status_code == 200:

  tt = convert_to_json(r.text)
  
  for train_stop in tt["timetable"]["s"]:
  
    try:
      db.timetable.update(
        {"@id": train_stop["@id"]},
        train_stop,
        upsert = True
        )
    except TypeError:
      pass

  
# RECENT CHANGES

# Triggern: Alle 2 Minuten

eva = 8004158 # 8000261: München, 8003680: Limburg Süd, 8004158: Pasing

url = f"https://api.deutschebahn.com/timetables/v1/rchg/{eva}"

r = requests.get(url, headers = headers)

recent_changes = convert_to_json(r.text)

for change in recent_changes["timetable"]["s"]:
  db.changes.update(
    {"@id": change["@id"]},
    change,
    upsert = True # insert if not exists
  )
  



### get /fchg/{evaNo} ###

# Timetable object (see Timetable) that contains all known changes for the station given by evaNo.
# The data includes all known changes from now on until indefinitely into the future. Once changes become obsolete (because their trip departs from the station) they are removed from this resource.
# Changes may include messages. On event level, they usually contain one or more of the 'changed' attributes ct, cp, cs or cpth. Changes may also include 'planned' attributes if there is no associated planned data for the change (e.g. an unplanned stop or trip).
# Full changes are updated every 30s and should be cached for that period by web caches.


eva = 8000261 # 8000261: München, 8003680: Limburg Süd
date = 210504
hour = 22

url = f"https://api.deutschebahn.com/timetables/v1/fchg/{eva}"

r = requests.get(url, headers = headers)



##########################################################################


# TAGS
# s: timetable stop
# ts: timestamp in YYMMddHHmm
# eva: EVA station number

# Grundstruktur:

# s: stop
    # tl: trip label
    # ar: arrival
    # dp: departure

{
  "station": "string",
  "eva": 0, # EVA station code
  "s": [ # timetable stop
    {
      "id": "string", # An id that uniquely identifies the stop. It consists of the following three elements separated by dashes: 1. a ‘daily trip id’ that uniquely identifies a trip within one day. This id is typically reused on subsequent days. This could be negative. 2. a 6-digit date specifier (YYMMdd) that indicates the planned departure date of the trip from its start station. 3. an index that indicates the position of the stop within the trip (in rare cases, one trip may arrive multiple times at one station). Example: “-7874571842864554321-1403311221-11” would be used for a trip with daily trip id “-7874571842864554321” that starts on march the 31th 2014 and where the current station is the 11th stop
      "eva": 0,
      "tl": { # trip label
        "f": "string", # filter flags. Siehe 1.2.26. D = external, F = long distance, N = regional, S = SBahn
        "t": "p", # trip type
        "o": "string", # Owner. A unique short-form and only intended to map a trip to specific evu.
        "n": "string",
        "c": "string"
      },
      "ref": { # Reference to an referenced trip.The substitution or additional trip references the originally planned trip. Note: referenced trip != reference trip
        "tl": { # The referred trips label
          "f": "string",
          "t": "p",
          "o": "string",
          "n": "string",
          "c": "string"
        },
        "rt": [ # The referred trips reference trip elements.
          {
            "f": "string",
            "t": "p",
            "o": "string",
            "n": "string",
            "c": "string"
          }
        ]
      },
      "ar": { # Arrival element.This element does not have child elements. All information about the arrival is stored in attrib-utes
        "ppth": "string", # Planned Path. A sequence of station names separated by the pipe symbols (“|”). For arrival, the path indicates the stations that come before the current station. The first ele-ment then is the trip’s start station. Note that the current station is never included in the path
        "cpth": "string", # changed path
        "pp": "string", # planned platform
        "cp": "string", # changed platform
        "pt": "string", # Planned time. Planned departure or arrival time
        "ct": "string", # Changed time. New estimated or actual departure or arrival time.
        "ps": "p", # Planned status
        "cs": "p", # Changed status. The status of this event, a one-character indicator that is one of:• “a” = this event was added• “c” = this event was cancelled• “p” = this event was planned (also used when the cancellation of an event has been revoked)Thestatus applies to the event, not to the trip as a whole. Insertion or removal of a single stop will usually affect two events at once: one arri-val and one departure event. Note that these two events do not have to belong to the same stop. For example, removing the last stop of a trip will result in arrival cancellation for the last stop and of departure cancellation for the stop before the last. So asymmetric cancellations of just arrival or departure for a stop can occur.
        "hi": 0, # Hidden.1 if the event should not be shown on WBT because travellers are not supposed to enter or exit the train at this stop.
        "clt": "string", # Cancellation time. Time when the cancellation of this stop was created.
        "wings": "string", # Wing. A sequence of trip id separated by the pipe symbols (“|”).E.g.: “-906407760000782942-1403311431”.
        "tra": "string", # Transition. Trip id of the next or previous train of a shared train. At the start stop this references the previous trip, at the last stop it references the next trip.E.g.: “2016448009055686515-1403311438-1
        "pde": "string", # Planned distant endpoint.
        "cde": "string", # Changed distant endpoint
        "dc": 0, # Distant change.
        "l": "string", # Line. The line indicator (e.g. "3" for an S-Bahn or "45S" for a bus).
        "m": [ # message
          {
            "id": "string",
            "t": "h",
            "from": "string",
            "to": "string",
            "c": 0,
            "int": "string",
            "ext": "string",
            "cat": "string",
            "ec": "string",
            "ts": "string",
            "pr": 1,
            "o": "string",
            "elnk": "string",
            "del": 0,
            "dm": [
              {
                "t": "s",
                "n": "string",
                "int": "string",
                "ts": "string"
              }
            ],
            "tl": [
              {
                "f": "string",
                "t": "p",
                "o": "string",
                "n": "string",
                "c": "string"
              }
            ]
          }
        ]
      },
      "dp": {  # Departure element.This element does not have child elements. All information about the departure is stored in at-tributes
        "ppth": "string", # Planned Path. A sequence of station names separated by the pipe symbols (“|”). For departure, the path indicates the stations that come after the current station. The last ele-ment in the path then is the trip’s destination station. The first ele-ment then is the trip’s start station. Note that the current station is never included in the path
        "cpth": "string", # changed path
        "pp": "string", # planned platform
        "cp": "string", # changed platform
        "pt": "string", # Planned time. Planned departure or arrival time
        "ct": "string", # Changed time. New estimated or actual depar-ture or arrival time.
        "ps": "p", # Planned status
        "cs": "p", # Changed status. The status of this event, a one-character indicator that is one of:• “a” = this event was added• “c” = this event was cancelled• “p” = this event was planned (also used when the cancellation of an event has been revoked)Thestatus applies to the event, not to the trip as a whole. Insertion or removal of a single stop will usually affect two events at once: one arri-val and one departure event. Note that these two events do not have to belong to the same stop. For example, removing the last stop of a trip will result in arrival cancellation for the last stop and of departure cancellation for the stop before the last. So asymmetric cancellations of just arrival or departure for a stop can occur.
        "hi": 0, # Hidden.1 if the event should not be shown on WBT because travellers are not supposed to enter or exit the train at this stop.
        "clt": "string", # Cancellation time. Time when the cancellation of this stop was created.
        "wings": "string", # Wing. A sequence of trip id separated by the pipe symbols (“|”).E.g.: “-906407760000782942-1403311431”.
        "tra": "string", # Transition. Trip id of the next or previous train of a shared train. At the start stop this references the previous trip, at the last stop it references the next trip.E.g.: “2016448009055686515-1403311438-1
        "pde": "string", # Planned distant endpoint.
        "cde": "string", # Changed distant endpoint
        "dc": 0, # Distant change.
        "l": "string", # Line. The line indicator (e.g. "3" for an S-Bahn or "45S" for a bus).
        "m": [ # message    
          {
            "id": "string",
            "t": "h",
            "from": "string",
            "to": "string",
            "c": 0,
            "int": "string",
            "ext": "string",
            "cat": "string",
            "ec": "string",
            "ts": "string",
            "pr": 1,
            "o": "string",
            "elnk": "string",
            "del": 0,
            "dm": [
              {
                "t": "s",
                "n": "string",
                "int": "string",
                "ts": "string"
              }
            ],
            "tl": [
              {
                "f": "string",
                "t": "p",
                "o": "string",
                "n": "string",
                "c": "string"
              }
            ]
          }
        ]
      },
      "m": [ # Message
        {
          "id": "string", # message id
          "t": "h", # message type. Siehe 1.2.21
          "from": "string", # valid from. The time, in ten digit “YYMMddHHmm” format
          "to": "string", # valid to. The time, in ten digit “YYMMddHHmm” format,
          "c": 0, # code. Siehe "2" List of all codes"
          "int": "string", # internal text
          "ext": "string", # external text
          "cat": "string", # category, zB "Information"
          "ec": "string", # external category
          "ts": "string", # timestamp
          "pr": 1, # priority. Siehe 1.2.22. 1 = HIGH, 3 = LOW, 4 = DONE
          "o": "string", # owner
          "elnk": "string", # external link associated with the message
          "del": 0, # deleted
          "dm": [ # distributor message: An additional message to a given station-based disruption by a specific distributor
            {
              "t": "s", # distributor type. Siehe 1.2.23. s = city, r = region, f = long distance, x = other
              "n": "string", # distributor name
              "int": "string", # internal text
              "ts": "string" # timestamp
            }
          ],
          "tl": [ # trip label
            {
              "f": "string",
              "t": "p",
              "o": "string",
              "n": "string",
              "c": "string"
            }
          ]
        }
      ],
      "hd": [ # historic delay: It’s the history of all delay-messages for a stop.
        {
          "ts": "string",
          "ar": "string", # The arrivalevent (YYMMddHHmm)
          "dp": "string", # The departureevent (YYMMddHHmm)
          "src": "L", # Source of the message
          "cod": "string" # Detailed description of delay cause
        }
      ],
      "hpc": [ # historic platform change: t’s the history of all platform-changes for a stop
        {
          "ts": "string",
          "ar": "string", # arrival platform
          "dp": "string", # departure platform
          "cot": "string" # detailed cause of track change
        }
      ],
      "conn": [ # connection. It’s information about a connected train at a particular stop. A Connection (German: Anschluss).
        {
          "id": "string",
          "ts": "string", # time stamp
          "eva": 0, # eva station number
          "cs": "w", # connection status (w = waiting, n = transition (this (regular) connection CANNOT wait), a = alternative(This is an alternative (unplanned) connection that has been introduced as a replacement for one regular connection that cannot wait. The connections "tl" (triplabel) attribute might in this case refer to the replaced connection (or more specifically the trip from that connection). Alter-native connections are always waiting (they are removed otherwise)))
          "ref": {}, # Timetable stop of missed trip
          "s": {} # Timetable stop
        }
      ],
      "rtr": [ # Reference trip relation. A reference trip relation holds how a reference trip is related to a stop, for instance the reference trip starts after the stop.
        {
          "rt": { # reference trip element. A reference trip is another real trip, but it doesn’t have its own stops and events. It refers only to its referenced regular trip. The reference trip collects mainly all different attributes of the referenced regular trip.
            "id": "string", # An id that uniquely identifies the reference trip. It consists of the following two elements sepa-rated by dashes:•A ‘daily trip id’ that uniquely identifies a refer-ence trip within one day. This id is typically re-used on subsequent days. This could be neg-ative.•A 10-digit date specifier (YYMMddHHmm) that indicates the planned departure date of the referenced regular trip from its start sta-tion.Example:“-7874571842864554321-1403311221” would be used for a trip with daily trip id “-7874571842864554321” that starts on march the 31th2014
            "c": true, # The cancellation flag. True means, the refer-ence trip is cancelled
            "rtl": { # Reference trip label
              "n": "string", # Trip/train number, e.g. "4523".
              "c": "string" # Category. Trip category, e.g. "ICE" or "RE".
            },
            "sd": { # Reference trip stop label of the start departure event
              "i": 0, # the index of the correspondent stop of the reg-ular trip
              "pt": "string", # The planned time of the correspondent stop of the regular trip
              "eva": 0, # The eva number of the correspondent stop ofthe regular trip.
              "n": "string" # The (long) name of the correspondent stop of the regular trip.
            },
            "ea": { # Reference trip stop label of the end arrival event.
              "i": 0,
              "pt": "string",
              "eva": 0,
              "n": "string"
            }
          },
          "rts": "b" # relation to stop element: The reference trips relation to the stop, which contains it. b = BEFORE (The reference trip ends before that stop), e = END (The reference trips ends at that stop), c = BETWEEN (The stop is between reference tripsstart and end, in other words, the stop is containedwithin its travel path.), s = START (The reference trip starts at that stop), a = AFTER (The reference trip starts after that stop.)
        }
      ]
    }
  ],
  "m": [ # message
    {
      "id": "string",
      "t": "h",
      "from": "string",
      "to": "string",
      "c": 0,
      "int": "string",
      "ext": "string",
      "cat": "string",
      "ec": "string",
      "ts": "string",
      "pr": 1,
      "o": "string",
      "elnk": "string",
      "del": 0,
      "dm": [
        {
          "t": "s",
          "n": "string",
          "int": "string",
          "ts": "string"
        }
      ],
      "tl": [
        {
          "f": "string",
          "t": "p",
          "o": "string",
          "n": "string",
          "c": "string"
        }
      ]
    }
  ]
}