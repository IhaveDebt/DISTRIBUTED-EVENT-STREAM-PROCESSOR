import Foundation

// MARK: - Distributed Event Stream Pipeline Simulation

struct Event {
    let id: UUID
    let payload: String
    let timestamp: Date
}

class EventNode {
    let name: String
    var queue: [Event] = []
    
    init(name: String) {
        self.name = name
    }
    
    func receive(_ event: Event) {
        queue.append(event)
        print("[\(name)] Received event \(event.id)")
    }
    
    func process() -> [Event] {
        let processed = queue.map { Event(id: $0.id,
                                         payload: $0.payload.uppercased(),
                                         timestamp: $0.timestamp) }
        queue.removeAll()
        return processed
    }
}

class EventBroker {
    var nodes: [EventNode] = []
    
    func register(node: EventNode) {
        nodes.append(node)
    }
    
    func broadcast(_ event: Event) {
        for node in nodes {
            node.receive(event)
        }
    }
    
    func collect() -> [Event] {
        return nodes.flatMap { $0.process() }
    }
}

let broker = EventBroker()
let nodes = [
    EventNode(name: "Parser"),
    EventNode(name: "Validator"),
    EventNode(name: "Analytics"),
    EventNode(name: "Archiver")
]

nodes.forEach { broker.register(node: $0) }

for i in 1...100 {
    let event = Event(id: UUID(),
                      payload: "event-\(i)",
                      timestamp: Date())
    broker.broadcast(event)
}

let results = broker.collect()
print("Processed Events: \(results.count)")
