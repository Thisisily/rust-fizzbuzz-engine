use std::sync::mpsc;
use std::thread;
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

// Trait for FizzBuzz rules
trait FizzBuzzRule: Send + Sync {
    fn apply(&self, n: u64) -> Option<String>;
    fn complexity(&self) -> u32; // Measure of rule complexity
}

// Struct for simple divisibility rules
struct DivisibilityRule {
    divisor: u64,
    output: String,
}

impl FizzBuzzRule for DivisibilityRule {
    fn apply(&self, n: u64) -> Option<String> {
        if n % self.divisor == 0 {
            Some(self.output.clone())
        } else {
            None
        }
    }
    fn complexity(&self) -> u32 { 1 }
}

// Struct for more complex rules (unnecessary complexity)
struct ComplexRule {
    predicate: Box<dyn Fn(u64) -> bool + Send + Sync>,
    output: String,
    complexity_score: u32,
}

impl FizzBuzzRule for ComplexRule {
    fn apply(&self, n: u64) -> Option<String> {
        if (self.predicate)(n) {
            Some(self.output.clone())
        } else {
            None
        }
    }
    fn complexity(&self) -> u32 { self.complexity_score }
}

// FizzBuzz microservice
struct FizzBuzzMicroservice {
    rules: Vec<Box<dyn FizzBuzzRule>>,
    cache: Arc<RwLock<HashMap<u64, String>>>,
}

impl FizzBuzzMicroservice {
    fn new() -> Self {
        FizzBuzzMicroservice { 
            rules: Vec::new(),
            cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    fn add_rule(&mut self, rule: Box<dyn FizzBuzzRule>) {
        self.rules.push(rule);
    }

    fn process(&self, n: u64) -> String {
        // Check cache first
        if let Some(cached_result) = self.cache.read().unwrap().get(&n) {
            return cached_result.clone();
        }

        let mut result = String::new();
        for rule in &self.rules {
            if let Some(output) = rule.apply(n) {
                result.push_str(&output);
            }
        }
        let final_result = if result.is_empty() { n.to_string() } else { result };

        // Update cache
        self.cache.write().unwrap().insert(n, final_result.clone());
        final_result
    }
}

// Event bus for inter-service communication
struct EventBus {
    subscribers: HashMap<String, Vec<mpsc::Sender<Event>>>,
}

impl EventBus {
    fn new() -> Self {
        EventBus { subscribers: HashMap::new() }
    }

    fn publish(&mut self, event: Event) {
        if let Some(subscribers) = self.subscribers.get(&event.event_type) {
            for subscriber in subscribers {
                let _ = subscriber.send(event.clone());
            }
        }
    }

    fn subscribe(&mut self, event_type: String, subscriber: mpsc::Sender<Event>) {
        self.subscribers.entry(event_type).or_insert_with(Vec::new).push(subscriber);
    }
}

// Event struct for pub/sub system
#[derive(Clone)]
struct Event {
    event_type: String,
    payload: u64,
}

// Worker thread function
fn worker(
    id: usize,
    service: Arc<FizzBuzzMicroservice>,
    rx: mpsc::Receiver<Event>,
    results: Arc<Mutex<HashMap<u64, String>>>,
    event_bus: Arc<Mutex<EventBus>>,
) {
    while let Ok(event) = rx.recv() {
        let n = event.payload;
        let result = service.process(n);
        results.lock().unwrap().insert(n, result.clone());

        // Publish result event
        event_bus.lock().unwrap().publish(Event {
            event_type: "result_computed".to_string(),
            payload: n,
        });

        // Simulate varying processing times
        thread::sleep(Duration::from_millis((id as u64 * n % 50) + 10));
    }
}

// Monitoring thread function
fn monitor(results: Arc<Mutex<HashMap<u64, String>>>, rx: mpsc::Receiver<Event>) {
    let mut last_print = Instant::now();
    let mut completed = 0;

    while let Ok(_) = rx.recv() {
        completed += 1;
        if last_print.elapsed() >= Duration::from_secs(1) {
            println!("Processed {} numbers. Cache size: {}", completed, results.lock().unwrap().len());
            last_print = Instant::now();
        }
    }
}

fn main() {
    let mut service = FizzBuzzMicroservice::new();
    service.add_rule(Box::new(DivisibilityRule { divisor: 3, output: "Fizz".to_string() }));
    service.add_rule(Box::new(DivisibilityRule { divisor: 5, output: "Buzz".to_string() }));
    service.add_rule(Box::new(ComplexRule {
        predicate: Box::new(|n| n.to_string().chars().sum::<u32>() % 7 == 0),
        output: "Whizz".to_string(),
        complexity_score: 3,
    }));
    
    let service = Arc::new(service);
    let results = Arc::new(Mutex::new(HashMap::new()));
    let event_bus = Arc::new(Mutex::new(EventBus::new()));
    
    let (tx, rx) = mpsc::channel();
    let rx = Arc::new(Mutex::new(rx));
    
    let num_threads = 8;
    let mut handles = vec![];
    
    // Spawn worker threads
    for id in 0..num_threads {
        let service = Arc::clone(&service);
        let results = Arc::clone(&results);
        let event_bus = Arc::clone(&event_bus);
        let rx = Arc::clone(&rx);
        
        let (worker_tx, worker_rx) = mpsc::channel();
        event_bus.lock().unwrap().subscribe("process_number".to_string(), worker_tx);
        
        handles.push(thread::spawn(move || {
            worker(id, service, worker_rx, results, event_bus);
        }));
    }
    
    // Spawn monitoring thread
    let (monitor_tx, monitor_rx) = mpsc::channel();
    event_bus.lock().unwrap().subscribe("result_computed".to_string(), monitor_tx);
    handles.push(thread::spawn(move || {
        monitor(Arc::clone(&results), monitor_rx);
    }));
    
    // Generate events
    for i in 1..=1000 {
        event_bus.lock().unwrap().publish(Event {
            event_type: "process_number".to_string(),
            payload: i,
        });
    }
    
    // Wait for all events to be processed
    thread::sleep(Duration::from_secs(10));
    
    // Print final results
    let results = results.lock().unwrap();
    for i in 1..=100 {
        println!("{}: {}", i, results.get(&i).unwrap_or(&"Not computed".to_string()));
    }
}
