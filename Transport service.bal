import ballerina/http;
import ballerina/uuid;
import ballerinax/mongodb;
import ballerinax/kafka;

type Route record {|
    string id;
    string name;
    string startPoint;
    string endPoint;
    string vehicleType; 
|};

type Trip record {|
    string id;
    string routeId;
    string departureTime;
    string arrivalTime;
    string status;
    decimal price;
|};

type ScheduleUpdate record {|
    string tripId;
    string status;
    string message;
|};

configurable string mongoUrl = "mongodb://admin:password@localhost:27017";
configurable string kafkaBootstrap = "localhost:9092";

mongodb:Client mongoClient = check new ({
    connection: {
        url: mongoUrl
    }
});

kafka:Producer scheduleProducer = check new (kafka:DEFAULT_URL, {
    clientId: "transport-schedule-producer",
    acks: "all",
    retryCount: 3
});

service /transport on new http:Listener(8082) {
    
    resource function post routes(@http:Payload Route routeReq) returns http:Created|error {
        mongodb:Database db = check mongoClient->getDatabase("transport_db");
        mongodb:Collection routes = check db->getCollection("routes");
        Route route = {
            id: uuid:createType1AsString(),
            name: routeReq.name,
            startPoint: routeReq.startPoint,
            endPoint: routeReq.endPoint,
            vehicleType: routeReq.vehicleType
        };
         check routes->insertOne(route);
        return http:CREATED;
    }
    
    resource function get routes() returns json[]|error {
        mongodb:Database db = check mongoClient->getDatabase("transport_db");
        mongodb:Collection routes = check db->getCollection("routes");
        stream<Route, error?> routeStream = check routes->find({});
        Route[]|error result = from Route r in routeStream select r;
        if result is Route[] {
            json[] routeList = [];
            foreach Route r in result {
                routeList.push({
                    "id": r.id,
                    "name": r.name,
                    "startPoint": r.startPoint,
                    "endPoint": r.endPoint,
                    "vehicleType": r.vehicleType
                });
            }
            return routeList;
        }
        return [];
    }
    
    resource function post trips(@http:Payload Trip tripReq) returns http:Created|error {
        mongodb:Database db = check mongoClient->getDatabase("transport_db");
        mongodb:Collection trips = check db->getCollection("trips");
        Trip trip = {
            id: uuid:createType1AsString(),
            routeId: tripReq.routeId,
            departureTime: tripReq.departureTime,
            arrivalTime: tripReq.arrivalTime,
            status: "scheduled",
            price: tripReq.price
        };
        check trips->insertOne(trip);
        return http:CREATED;
    }
    
    resource function get routes/[string routeId]/trips() returns json[]|error {
        mongodb:Database db = check mongoClient->getDatabase("transport_db");
        mongodb:Collection trips = check db->getCollection("trips");
        map<json> filter = {"routeId": routeId};
        stream<Trip, error?> tripStream = check trips->find(filter);
        Trip[]|error result = from Trip t in tripStream select t;
        if result is Trip[] {
            json[] tripList = [];
            foreach Trip t in result {
                tripList.push({
                    "id": t.id,
                    "routeId": t.routeId,
                    "departureTime": t.departureTime,
                    "arrivalTime": t.arrivalTime,
                    "status": t.status,
                    "price": t.price
                });
            }
            return tripList;
        }
         return [];
    }
    
  resource function put trips/[string tripId]/status(string status, string message) returns json|error {
        mongodb:Database db = check mongoClient->getDatabase("transport_db");
        mongodb:Collection trips = check db->getCollection("trips");
        map<json> filter = {"id": tripId};
        map<json> update = {"$set": {"status": status}};
        mongodb:UpdateResult updateResult = check trips->updateOne(filter, update);
        if updateResult.modifiedCount > 0 {
          ScheduleUpdate scheduleUpdate = {
                tripId: tripId,
                status: status,
                message: message
            };
            check scheduleProducer->send({
                topic: "schedule.updates",
                value: scheduleUpdate.toJsonString().toBytes()
            });
            return {"message": "Trip status updated and notification sent"};
        }
        return {"error": "Trip not found"};
    }
}
