import ballerina/http;
import ballerinax/mongodb;
import ballerinax/kafka;

type SalesReport record {|
    int totalTickets;
    decimal totalRevenue;
    map<int> ticketsByType;
    map<decimal> revenueByType;
|};

type DisruptionEvent record {|
    string routeId;
    string message;
    string severity;
|};

configurable string mongoUrl = "mongodb://admin:password@localhost:27017";
configurable string kafkaBootstrap = "localhost:9092";

mongodb:Client mongoClient = check new ({
    connection: {
        url: mongoUrl
    }
});

kafka:Producer adminProducer = check new (kafka:DEFAULT_URL, {
    clientId: "admin-producer",
    acks: "all",
    retryCount: 3
});

service /admin on new http:Listener(8086) {
    
    resource function get reports/sales() returns json|error {
        mongodb:Database db = check mongoClient->getDatabase("transport_db");
        mongodb:Collection tickets = check db->getCollection("tickets");
        map<json> filter = {"status": "PAID"};
        stream<record {string ticketType; decimal price;}, error?> ticketStream = 
            check tickets->find(filter, {}, {}, {"ticketType": 1, "price": 1});
        record {string ticketType; decimal price;}[]|error result = 
            from var t in ticketStream select t;
        
        if result is record {string ticketType; decimal price;}[] {
            int totalTickets = result.length();
            decimal totalRevenue = 0.0;
            map<int> ticketsByType = {};
            map<decimal> revenueByType = {};
            
            foreach var ticket in result {
                totalRevenue += ticket.price;
                if ticketsByType.hasKey(ticket.ticketType) {
                    ticketsByType[ticket.ticketType] = ticketsByType.get(ticket.ticketType) + 1;
                } else {
                    ticketsByType[ticket.ticketType] = 1;
                }
                 if revenueByType.hasKey(ticket.ticketType) {
                    revenueByType[ticket.ticketType] = revenueByType.get(ticket.ticketType) + ticket.price;
                } else {
                    revenueByType[ticket.ticketType] = ticket.price;
                }
            }
             SalesReport report = {
                totalTickets: totalTickets,
                totalRevenue: totalRevenue,
                ticketsByType: ticketsByType,
                revenueByType: revenueByType
            };
             return report.toJson();
        }
         return {
            "totalTickets": 0,
            "totalRevenue": 0.0,
            "ticketsByType": {},
            "revenueByType": {}
        };
    }
    
    resource function get reports/traffic() returns json|error {
        mongodb:Database db = check mongoClient->getDatabase("transport_db");
        mongodb:Collection tickets = check db->getCollection("tickets");
        map<json> filter = {"status": "VALIDATED"};
        stream<record {string tripId;}, error?> ticketStream = 
            check tickets->find(filter, {}, {}, {"tripId": 1});
        record {string tripId;}[]|error result = 
            from var t in ticketStream select t;
        
        if result is record {string tripId;}[] {
            map<int> trafficByTrip = {};
            
            foreach var ticket in result {
                if trafficByTrip.hasKey(ticket.tripId) {
                    trafficByTrip[ticket.tripId] = trafficByTrip.get(ticket.tripId) + 1;
                } else {
                    trafficByTrip[ticket.tripId] = 1;
                }
            }
            return {"trafficByTrip": trafficByTrip};
        }
         return {"trafficByTrip": {}};
    }
    
    resource function post disruptions(@http:Payload DisruptionEvent disruption) returns json|error {
    
        json disruptionEvent = {
            "routeId": disruption.routeId,
            "message": disruption.message,
            "severity": disruption.severity,
            "timestamp": check getCurrentTimestamp()
        };
         check adminProducer->send({
            topic: "schedule.updates",
            value: disruptionEvent.toJsonString().toBytes()
        });
        
        return {"message": "Disruption notification published successfully"};
    }
    
    
    resource function get routes/summary() returns json[]|error {
        mongodb:Database db = check mongoClient->getDatabase("transport_db");
        mongodb:Collection routes = check db->getCollection("routes");
        mongodb:Collection trips = check db->getCollection("trips");
        
        stream<record {string id; string name; string startPoint; string endPoint; string vehicleType;}, error?> routeStream = 
            check routes->find({});
        record {string id; string name; string startPoint; string endPoint; string vehicleType;}[]|error routeResult = 
            from var r in routeStream select r;
        if routeResult is record {string id; string name; string startPoint; string endPoint; string vehicleType;}[] {
            json[] summary = [];
            
            foreach var route in routeResult {
                map<json> tripFilter = {"routeId": route.id};
                int tripCount = check trips->countDocuments(tripFilter);
                summary.push({
                    "routeId": route.id,
                    "name": route.name,
                    "startPoint": route.startPoint,
                    "endPoint": route.endPoint,
                    "vehicleType": route.vehicleType,
                    "tripCount": tripCount
                });
            }
            return summary;
        }
         return [];
    }
    
    resource function get statistics/tickets() returns json|error {
        mongodb:Database db = check mongoClient->getDatabase("transport_db");
        mongodb:Collection tickets = check db->getCollection("tickets");
         int totalTickets = check tickets->countDocuments({});
        int createdTickets = check tickets->countDocuments({"status": "CREATED"});
        int paidTickets = check tickets->countDocuments({"status": "PAID"});
        int validatedTickets = check tickets->countDocuments({"status": "VALIDATED"});
        int expiredTickets = check tickets->countDocuments({"status": "EXPIRED"});
        
        return {
            "total": totalTickets,
            "created": createdTickets,
            "paid": paidTickets,
            "validated": validatedTickets,
            "expired": expiredTickets
        };
    }
}

function getCurrentTimestamp() returns string|error {
    return "2025-10-05T12:00:00Z";
}
