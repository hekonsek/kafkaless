# KKipes Kafka binding

This project provides Java implementation of KPipes patterns for data and service events.

## KPipes patterns

### Data events

Data event is a piece of data stored as Kafka binary message and identified by unique `string` key.

    [myKey1][myBytes1]
    [myKey2][myBytes2]    
    [myKey3][myBytes3]
    
Stream of data events are named and stored in multi-tenant fashion in topic `data.TENANT.NAMESPACE.NAME`. So for example stream of 
data events associated with tenant `acme` and representing cars of user `john` can be stored in `data.acme.john.car` topic.