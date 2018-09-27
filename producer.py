import json, time, datetime, random
from kafka import KafkaProducer
from kafka.errors import KafkaError
from random import uniform
   
#para generar fechas random
def strTimeProp(start, end, format, prop):
    """Get a time at a proportion of a range of two formatted times.

    start and end should be strings specifying times formated in the
    given format (strftime-style), giving an interval [start, end].
    prop specifies how a proportion of the interval to be taken after
    start.  The returned time will be in the specified format.
    """

    stime = time.mktime(time.strptime(start, format))
    etime = time.mktime(time.strptime(end, format))

    ptime = stime + prop * (etime - stime)

    return time.strftime(format, time.localtime(ptime))


def randomDate(start, end, prop):
    return strTimeProp(start, end, '%d/%m/%Y %I:%M %p', prop)


producer = KafkaProducer(bootstrap_servers=['172.24.41.165:8081'], 
						 value_serializer=lambda v: json.dumps(v).encode('utf-8'))

while True:
	producer.send('PuenteAranda-Bochica-ParqueoFacil', {'fechaReserva': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 'demandante': 'individual', 
					'fechaInicio': randomDate("27/9/2018 12:01 AM", "30/9/2018 11:59 PM", random.random()),'fechaFin': randomDate("2/10/2018 12:01 AM", "5/10/2018 11:59 PM", random.random()), 
                    'horaLlegada': str(int(round(uniform(0, 12),0))) + ":" + str(int(round(uniform(0, 59),0))), 'horaSalida': str(int(round(uniform(13, 23),0))) + ":" + str(int(round(uniform(0, 59),0))),
                    'lugar': 'PuenteAranda/Bochica', 'valorAPagar':round(uniform(7000, 50000),0)})
	producer.flush()
	time.sleep(5)
    