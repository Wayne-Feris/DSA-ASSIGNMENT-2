import ballerina/http;
import ballerina/uuid;
import ballerinax/mongodb;
import ballerinax/kafka;

type Ticket record {|
    string id;
    string passengerId;
    string tripId;
    string ticketType; 
    string status;
    decimal price;
    int ridesRemaining?;
|};

type TicketRequest record {|
    string passengerId;
    string tripId;
    string ticketType;
    int rides?;
|};

type PaymentEvent record {|
    string ticketId;
    string status;
    decimal amount;
|};

configurable string mongoUrl = "mongodb://admin:password@localhost:27017";
configurable string kafkaBootstrap = "localhost:9092";

mongodb:Client mongoClient = check new ({
    connection: {
        url: mongoUrl
    }
});

kafka:Producer ticketProducer = check new (kafka:DEFAULT_URL, {
    clientId: "ticketing-producer",
    acks: "all",
    retryCount: 3
});

kafka:Listener paymentListener = check new (kafka:DEFAULT_URL, {
    groupId: "ticketing-consumer-group",
    topics: ["payments.processed"]
});

service /ticketing on new http:Listener(8083) {
     resource function post tickets(@http:Payload TicketRequest req) returns json|error {
        mongodb:Database db = check mongoClient->getDatabase("transport_db");
        mongodb:Collection tickets = check db->getCollection("tickets");
         mongodb:Collection trips = check db->getCollection("trips");
        map<json> tripFilter = {"id": req.tripId};
        stream<record {decimal price;}, error?> tripStream = check trips->find(tripFilter, {}, {}, {"price": 1});
        record {decimal price;}[]|error tripResult = from var t in tripStream select t;
        decimal price = 10.0; 
        if tripResult is record {decimal price;}[] && tripResult.length() > 0 {
            price = tripResult[0].price;
        }
        if req.ticketType == "multiple" && req.rides is int {
            price = price * <decimal>req.rides * 0.9;
        } else if req.ticketType == "pass" {
            price = 100.0;
        }
        Ticket ticket = {
            id: uuid:createType1AsString(),
            passengerId: req.passengerId,
            tripId: req.tripId,
            ticketType: req.ticketType,
            status: "CREATED",
            price: price
        };
        if req.ticketType == "multiple" && req.rides is int {
            ticket.ridesRemaining = req.rides;
        }
        check tickets->insertOne(ticket);
        check ticketProducer->send({
            topic: "ticket.requests",
            value: ticket.toJsonString().toBytes()
        });
        
         return {
            "ticketId": ticket.id,
            "status": ticket.status,
            "price": ticket.price
        };
    }
    
    resource function get tickets/[string ticketId]() returns json|http:NotFound|error {
        mongodb:Database db = check mongoClient->getDatabase("transport_db");
        mongodb:Collection tickets = check db->getCollection("tickets");
         map<json> filter = {"id": ticketId};
        stream<Ticket, error?> ticketStream = check tickets->find(filter);
        Ticket[]|error result = from Ticket t in ticketStream select t;
        if result is Ticket[] && result.length() > 0 {
            return result[0].toJson();
        }
        return http:NOT_FOUND;
    }
    
    resource function post tickets/[string ticketId]/validate() returns json|error {
        mongodb:Database db = check mongoClient->getDatabase("transport_db");
        mongodb:Collection tickets = check db->getCollection("tickets");
        map<json> filter = {"id": ticketId, "status": "PAID"};
        stream<Ticket, error?> ticketStream = check tickets->find(filter);
        Ticket[]|error result = from Ticket t in ticketStream select t;
         if result is Ticket[] && result.length() == 0 {
            return {"error": "Ticket not found or not paid"};
        }
        Ticket ticket = (<Ticket[]>result)[0];
         if ticket.ticketType == "single" {
            map<json> update = {"$set": {"status": "VALIDATED"}};
            check tickets->updateOne(filter, update);
        } else if ticket.ticketType == "multiple" {
            int remaining = ticket.ridesRemaining ?: 0;
            if remaining > 1 {
                map<json> update = {"$inc": {"ridesRemaining": -1}};
                check tickets->updateOne(filter, update);
            } else {
                map<json> update = {"$set": {"status": "VALIDATED", "ridesRemaining": 0}};
                check tickets->updateOne(filter, update);
            }
        }
        
        check ticketProducer->send({
            topic: "ticket.validated",
            value: string `{"ticketId": "${ticketId}", "passengerId": "${ticket.passengerId}"}`.toBytes()
        });
         return {"message": "Ticket validated successfully"};
    }
    
     resource function get passengers/[string passengerId]/tickets() returns json[]|error {
        mongodb:Database db = check mongoClient->getDatabase("transport_db");
        mongodb:Collection tickets = check db->getCollection("tickets");
         map<json> filter = {"passengerId": passengerId};
        stream<Ticket, error?> ticketStream = check tickets->find(filter);
        Ticket[]|error result = from Ticket t in ticketStream select t;
        if result is Ticket[] {
            json[] ticketList = [];
            foreach Ticket t in result {
                ticketList.push(t.toJson());
            }
            return ticketList;
        }
        return [];
    }
}

service on paymentListener {
     remote function onConsumerRecord(kafka:Caller caller, kafka:ConsumerRecord[] records) returns error? {
        mongodb:Database db = check mongoClient->getDatabase("transport_db");
        mongodb:Collection tickets = check db->getCollection("tickets");
        foreach kafka:ConsumerRecord rec in records {
            byte[] value = rec.value;
            string paymentData = check string:fromBytes(value);
            json paymentJson = check paymentData.fromJsonString();
             string ticketId = check paymentJson.ticketId;
            string status = check paymentJson.status;
             if status == "SUCCESS" {
                map<json> filter = {"id": ticketId};
                map<json> update = {"$set": {"status": "PAID"}};
                check tickets->updateOne(filter, update);
            }
        }
    }
}
