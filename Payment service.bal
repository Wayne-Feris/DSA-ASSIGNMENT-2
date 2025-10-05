import ballerina/http;
import ballerina/uuid;
import ballerina/random;
import ballerinax/mongodb;
import ballerinax/kafka;

type Payment record {|
    string id;
    string ticketId;
    decimal amount;
    string status;
    string timestamp;
|};

configurable string mongoUrl = "mongodb://admin:password@localhost:27017";
configurable string kafkaBootstrap = "localhost:9092";

mongodb:Client mongoClient = check new ({
    connection: {
        url: mongoUrl
    }
});

kafka:Producer paymentProducer = check new (kafka:DEFAULT_URL, {
    clientId: "payment-producer",
    acks: "all",
    retryCount: 3
});

kafka:Listener ticketListener = check new (kafka:DEFAULT_URL, {
    groupId: "payment-consumer-group",
    topics: ["ticket.requests"]
});

service /payment on new http:Listener(8084) {
    
    resource function post process(string ticketId, decimal amount) returns json|error {
        mongodb:Database db = check mongoClient->getDatabase("transport_db");
        mongodb:Collection payments = check db->getCollection("payments");
        int randomNum = check random:createIntInRange(1, 11);
        string status = randomNum > 2 ? "SUCCESS" : "FAILED"; 
        Payment payment = {
            id: uuid:createType1AsString(),
            ticketId: ticketId,
            amount: amount,
            status: status,
            timestamp: check getCurrentTimestamp()
        };
        check payments->insertOne(payment);
        json paymentEvent = {
            "ticketId": ticketId,
            "status": status,
            "amount": amount,
            "paymentId": payment.id
        };
        check paymentProducer->send({
            topic: "payments.processed",
            value: paymentEvent.toJsonString().toBytes()
        });
         return paymentEvent;
    }
    
     resource function get [string paymentId]() returns json|http:NotFound|error {
        mongodb:Database db = check mongoClient->getDatabase("transport_db");
        mongodb:Collection payments = check db->getCollection("payments");
         map<json> filter = {"id": paymentId};
        stream<Payment, error?> paymentStream = check payments->find(filter);
        Payment[]|error result = from Payment p in paymentStream select p;
        if result is Payment[] && result.length() > 0 {
            return result[0].toJson();
        }
         return http:NOT_FOUND;
    }
}

service on ticketListener {
    remote function onConsumerRecord(kafka:Caller caller, kafka:ConsumerRecord[] records) returns error? {
        mongodb:Database db = check mongoClient->getDatabase("transport_db");
        mongodb:Collection payments = check db->getCollection("payments");
         foreach kafka:ConsumerRecord rec in records {
            byte[] value = rec.value;
            string ticketData = check string:fromBytes(value);
            json ticketJson = check ticketData.fromJsonString();
             string ticketId = check ticketJson.id;
            decimal price = check ticketJson.price;
            int randomNum = check random:createIntInRange(1, 11);
            string status = randomNum > 2 ? "SUCCESS" : "FAILED";
             Payment payment = {
                id: uuid:createType1AsString(),
                ticketId: ticketId,
                amount: price,
                status: status,
                timestamp: check getCurrentTimestamp()
            };
            check payments->insertOne(payment);

             json paymentEvent = {
                "ticketId": ticketId,
                "status": status,
                "amount": price,
                "paymentId": payment.id
            };
             check paymentProducer->send({
                topic: "payments.processed",
                value: paymentEvent.toJsonString().toBytes()
            });
        }
    }
}

function getCurrentTimestamp() returns string|error {
  return "2025-10-05T12:00:00Z";
}
