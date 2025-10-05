import ballerina/http;
import ballerina/log;
import ballerinax/kafka;

configurable string kafkaBootstrap = "localhost:9092";

kafka:Listener notificationListener = check new (kafka:DEFAULT_URL, {
    groupId: "notification-consumer-group",
    topics: ["schedule.updates", "ticket.validated", "payments.processed"]
});

service /notification on new http:Listener(8085) {
    resource function get health() returns json {
        return {"status": "Notification service is running"};
    }
}

service on notificationListener {
    remote function onConsumerRecord(kafka:Caller caller, kafka:ConsumerRecord[] records) returns error? {
        foreach kafka:ConsumerRecord rec in records {
            byte[] value = rec.value;
            string eventData = check string:fromBytes(value);
            json eventJson = check eventData.fromJsonString();
            string topic = rec.topic;
             match topic {
                "schedule.updates" => {
                    string tripId = check eventJson.tripId;
                    string status = check eventJson.status;
                    string message = check eventJson.message;
                    log:printInfo(string `NOTIFICATION: Trip ${tripId} status changed to ${status}. Message: ${message}`);
                }
                "ticket.validated" => {
                    string ticketId = check eventJson.ticketId;
                    string passengerId = check eventJson.passengerId;
                    log:printInfo(string `NOTIFICATION: Ticket ${ticketId} validated for passenger ${passengerId}`);
                  }
                "payments.processed" => {
                    string ticketId = check eventJson.ticketId;
                    string paymentStatus = check eventJson.status;
                    log:printInfo(string `NOTIFICATION: Payment for ticket ${ticketId} - Status: ${paymentStatus}`);
                }
                _ => {
                    log:printWarn(string `Unknown topic: ${topic}`);
                }
            }
        }
    }
}
