"""
=============================================================================
Mock Log Generator — GenAI Log Analysis POC
=============================================================================

Generates realistic structured JSON logs to Kafka topic 'app-logs' that mimic
what real Spring Boot microservices would produce.

Each log contains:
  - timestamp     (ISO 8601 UTC)
  - level         (INFO, WARN, ERROR)
  - service       (order-service, payment-service, inventory-service, etc.)
  - trace_id      (groups logs for one transaction)
  - order_no      (the key identifier — what users will ask about)
  - location_no   (warehouse location)
  - customer_email
  - message       (the log message)
  - exception     (full stack trace when applicable)

Realistic failure scenarios baked in:
  1. Database connection failures (timeout, pool exhausted, refused)
  2. NullPointerException in business logic
  3. Payment gateway timeouts
  4. Circuit breaker OPEN scenarios
  5. Inventory mismatches
  6. Network errors to downstream services
  7. JSON parse errors
  8. Out of memory situations
  9. Stale cache returning wrong data
  10. Race conditions in concurrent updates

Each scenario emits MULTIPLE correlated logs (5-15 per trace_id) so the
GenAI agent can later piece together the full story.

Usage:
    # Continuous mode (default) — generates 5 logs/sec forever
    python generate_logs.py

    # Burst mode — generates N transactions and exits
    python generate_logs.py --transactions 100 --burst

    # Custom rate
    python generate_logs.py --rate 10  # 10 logs/sec
"""

import argparse
import asyncio
import json
import random
import uuid
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any
from aiokafka import AIOKafkaProducer


# =============================================================================
# CONFIGURATION
# =============================================================================
KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC = "app-logs"

# Match Postgres seed data
LOCATIONS = [
    "LOC-NYC-01", "LOC-LAX-01", "LOC-CHI-01", "LOC-LON-01", "LOC-FRA-01",
    "LOC-MUM-01", "LOC-BLR-01", "LOC-SGP-01", "LOC-SYD-01", "LOC-TOR-01",
]

SERVICES = [
    "order-service",
    "payment-service",
    "inventory-service",
    "shipping-service",
    "notification-service",
    "fraud-detection-service",
]

# 200 orders pre-seeded in Postgres (ORD-00001 to ORD-00200)
ORDER_RANGE = (1, 200)


# =============================================================================
# SCENARIO BUILDERS — each returns a list of correlated log entries
# =============================================================================

def _base(trace_id: str, order_no: str, location_no: str,
          customer_email: str, ts: datetime) -> Dict[str, Any]:
    """Shared base fields for every log entry."""
    return {
        "timestamp": ts.isoformat(),
        "trace_id": trace_id,
        "order_no": order_no,
        "location_no": location_no,
        "customer_email": customer_email,
        "environment": "prod",
        "host": f"app-{random.randint(1, 8):02d}",
    }


def scenario_happy_path(order_no: str, location_no: str,
                        customer_email: str, t0: datetime) -> List[Dict]:
    """Successful order: 7 logs across 3 services. ~80% of transactions."""
    trace_id = f"trace-{uuid.uuid4().hex[:12]}"
    logs = []

    logs.append({
        **_base(trace_id, order_no, location_no, customer_email, t0),
        "level": "INFO", "service": "order-service",
        "message": f"Received order request for {order_no} from {customer_email}",
    })
    logs.append({
        **_base(trace_id, order_no, location_no, customer_email,
                t0 + timedelta(milliseconds=120)),
        "level": "INFO", "service": "inventory-service",
        "message": f"Inventory check passed for order {order_no} at {location_no}",
    })
    logs.append({
        **_base(trace_id, order_no, location_no, customer_email,
                t0 + timedelta(milliseconds=350)),
        "level": "INFO", "service": "payment-service",
        "message": f"Initiating payment for order {order_no}",
    })
    logs.append({
        **_base(trace_id, order_no, location_no, customer_email,
                t0 + timedelta(milliseconds=1850)),
        "level": "INFO", "service": "payment-service",
        "message": f"Payment captured successfully for order {order_no}, txn_id=TXN{random.randint(100000, 999999)}",
    })
    logs.append({
        **_base(trace_id, order_no, location_no, customer_email,
                t0 + timedelta(milliseconds=2100)),
        "level": "INFO", "service": "order-service",
        "message": f"Order {order_no} confirmed, status=COMPLETED",
    })
    logs.append({
        **_base(trace_id, order_no, location_no, customer_email,
                t0 + timedelta(milliseconds=2400)),
        "level": "INFO", "service": "shipping-service",
        "message": f"Shipment SHP-{order_no.split('-')[1]} created for order {order_no} via FedEx",
    })
    logs.append({
        **_base(trace_id, order_no, location_no, customer_email,
                t0 + timedelta(milliseconds=2700)),
        "level": "INFO", "service": "notification-service",
        "message": f"Confirmation email sent to {customer_email} for order {order_no}",
    })
    return logs


def scenario_db_connection_failure(order_no: str, location_no: str,
                                   customer_email: str, t0: datetime) -> List[Dict]:
    """DB connection pool exhausted leading to order failure."""
    trace_id = f"trace-{uuid.uuid4().hex[:12]}"
    logs = []

    logs.append({
        **_base(trace_id, order_no, location_no, customer_email, t0),
        "level": "INFO", "service": "order-service",
        "message": f"Received order request for {order_no} from {customer_email}",
    })
    # Warning signs preceding the failure
    logs.append({
        **_base(trace_id, order_no, location_no, customer_email,
                t0 + timedelta(milliseconds=50)),
        "level": "WARN", "service": "order-service",
        "message": "HikariCP pool utilization at 95%, active=48/50 idle=2",
    })
    logs.append({
        **_base(trace_id, order_no, location_no, customer_email,
                t0 + timedelta(milliseconds=120)),
        "level": "WARN", "service": "order-service",
        "message": f"Awaiting available connection from pool (waited 5000ms)",
    })
    logs.append({
        **_base(trace_id, order_no, location_no, customer_email,
                t0 + timedelta(milliseconds=10120)),
        "level": "ERROR", "service": "order-service",
        "message": f"Failed to obtain JDBC Connection for order {order_no}",
        "exception": (
            "org.springframework.jdbc.CannotGetJdbcConnectionException: "
            "Failed to obtain JDBC Connection; nested exception is "
            "java.sql.SQLTransientConnectionException: HikariPool-1 - "
            "Connection is not available, request timed out after 10000ms.\n"
            "    at org.springframework.jdbc.datasource.DataSourceUtils.getConnection\n"
            "    at com.example.order.OrderRepository.save(OrderRepository.java:87)\n"
            "    at com.example.order.OrderService.createOrder(OrderService.java:142)\n"
            "    at com.example.order.OrderController.create(OrderController.java:56)"
        ),
    })
    logs.append({
        **_base(trace_id, order_no, location_no, customer_email,
                t0 + timedelta(milliseconds=10250)),
        "level": "ERROR", "service": "order-service",
        "message": f"Order {order_no} FAILED: database unavailable",
    })
    logs.append({
        **_base(trace_id, order_no, location_no, customer_email,
                t0 + timedelta(milliseconds=10400)),
        "level": "INFO", "service": "notification-service",
        "message": f"Failure notification sent for order {order_no}",
    })
    return logs


def scenario_null_pointer(order_no: str, location_no: str,
                         customer_email: str, t0: datetime) -> List[Dict]:
    """NullPointerException in business logic."""
    trace_id = f"trace-{uuid.uuid4().hex[:12]}"
    logs = []

    logs.append({
        **_base(trace_id, order_no, location_no, customer_email, t0),
        "level": "INFO", "service": "order-service",
        "message": f"Received order request for {order_no}",
    })
    logs.append({
        **_base(trace_id, order_no, location_no, customer_email,
                t0 + timedelta(milliseconds=80)),
        "level": "DEBUG", "service": "payment-service",
        "message": f"Validating payment method for order {order_no}",
    })
    logs.append({
        **_base(trace_id, order_no, location_no, customer_email,
                t0 + timedelta(milliseconds=130)),
        "level": "ERROR", "service": "payment-service",
        "message": f"Unexpected error processing payment for order {order_no}",
        "exception": (
            "java.lang.NullPointerException: Cannot invoke "
            "\"com.example.payment.PaymentMethod.getCardNumber()\" because "
            "\"paymentMethod\" is null\n"
            "    at com.example.payment.PaymentValidator.validate(PaymentValidator.java:64)\n"
            "    at com.example.payment.PaymentService.processPayment(PaymentService.java:112)\n"
            "    at com.example.payment.PaymentController.process(PaymentController.java:43)\n"
            "    at jdk.internal.reflect.GeneratedMethodAccessor42.invoke(Unknown Source)\n"
            "    at java.base/java.lang.reflect.Method.invoke(Method.java:568)"
        ),
    })
    logs.append({
        **_base(trace_id, order_no, location_no, customer_email,
                t0 + timedelta(milliseconds=200)),
        "level": "ERROR", "service": "order-service",
        "message": f"Order {order_no} FAILED: payment service returned 500",
    })
    return logs


def scenario_payment_gateway_timeout(order_no: str, location_no: str,
                                    customer_email: str, t0: datetime) -> List[Dict]:
    """Payment Service Provider gateway timeout — classic failure pattern."""
    trace_id = f"trace-{uuid.uuid4().hex[:12]}"
    logs = []

    logs.append({
        **_base(trace_id, order_no, location_no, customer_email, t0),
        "level": "INFO", "service": "order-service",
        "message": f"Received order request for {order_no}",
    })
    logs.append({
        **_base(trace_id, order_no, location_no, customer_email,
                t0 + timedelta(milliseconds=200)),
        "level": "INFO", "service": "payment-service",
        "message": f"Initiating payment for order {order_no}, amount={random.randint(50,500)}.00 USD",
    })
    # Early warning signs
    logs.append({
        **_base(trace_id, order_no, location_no, customer_email,
                t0 + timedelta(milliseconds=250)),
        "level": "WARN", "service": "psp-adapter",
        "message": "PSP Stripe response latency P99=4800ms (threshold 3000ms)",
    })
    logs.append({
        **_base(trace_id, order_no, location_no, customer_email,
                t0 + timedelta(milliseconds=5250)),
        "level": "WARN", "service": "payment-service",
        "message": f"PSP gateway timeout after 5000ms for order {order_no}, attempt=1/3",
    })
    logs.append({
        **_base(trace_id, order_no, location_no, customer_email,
                t0 + timedelta(milliseconds=10300)),
        "level": "WARN", "service": "payment-service",
        "message": f"PSP gateway timeout after 5000ms for order {order_no}, attempt=2/3",
    })
    logs.append({
        **_base(trace_id, order_no, location_no, customer_email,
                t0 + timedelta(milliseconds=10500)),
        "level": "ERROR", "service": "circuit-breaker",
        "message": "Circuit breaker OPEN for payment-service (failure rate 75% > threshold 50%)",
    })
    logs.append({
        **_base(trace_id, order_no, location_no, customer_email,
                t0 + timedelta(milliseconds=10550)),
        "level": "ERROR", "service": "payment-service",
        "message": f"Payment failed for order {order_no} after 2 retry attempts",
        "exception": (
            "com.example.payment.PaymentGatewayException: PSP gateway unavailable, "
            "circuit breaker OPEN\n"
            "    at com.example.payment.PspAdapter.charge(PspAdapter.java:198)\n"
            "    at com.example.payment.PaymentService.processPayment(PaymentService.java:145)\n"
            "    at com.example.payment.PaymentController.process(PaymentController.java:43)"
        ),
    })
    logs.append({
        **_base(trace_id, order_no, location_no, customer_email,
                t0 + timedelta(milliseconds=10700)),
        "level": "ERROR", "service": "order-service",
        "message": f"Order {order_no} FAILED: payment unavailable",
    })
    return logs


def scenario_inventory_mismatch(order_no: str, location_no: str,
                               customer_email: str, t0: datetime) -> List[Dict]:
    """Stale cache shows item in stock, actual inventory is zero."""
    trace_id = f"trace-{uuid.uuid4().hex[:12]}"
    sku = f"SKU-{random.randint(10000, 99999)}"
    logs = []

    logs.append({
        **_base(trace_id, order_no, location_no, customer_email, t0),
        "level": "INFO", "service": "order-service",
        "message": f"Received order request for {order_no}",
    })
    logs.append({
        **_base(trace_id, order_no, location_no, customer_email,
                t0 + timedelta(milliseconds=80)),
        "level": "INFO", "service": "inventory-service",
        "message": f"Cache HIT for {sku} at {location_no}, available=12 (cached 4m ago)",
    })
    logs.append({
        **_base(trace_id, order_no, location_no, customer_email,
                t0 + timedelta(milliseconds=120)),
        "level": "INFO", "service": "inventory-service",
        "message": f"Reserving inventory for order {order_no}, sku={sku}, qty=1",
    })
    logs.append({
        **_base(trace_id, order_no, location_no, customer_email,
                t0 + timedelta(milliseconds=180)),
        "level": "WARN", "service": "inventory-service",
        "message": f"Database actual count for {sku} at {location_no} is 0 (cache stale)",
    })
    logs.append({
        **_base(trace_id, order_no, location_no, customer_email,
                t0 + timedelta(milliseconds=220)),
        "level": "ERROR", "service": "inventory-service",
        "message": f"Inventory reservation FAILED for order {order_no}",
        "exception": (
            "com.example.inventory.InsufficientStockException: "
            f"Requested 1 of {sku} at {location_no}, available=0\n"
            "    at com.example.inventory.InventoryService.reserve(InventoryService.java:156)\n"
            "    at com.example.order.OrderService.createOrder(OrderService.java:178)"
        ),
    })
    logs.append({
        **_base(trace_id, order_no, location_no, customer_email,
                t0 + timedelta(milliseconds=260)),
        "level": "ERROR", "service": "order-service",
        "message": f"Order {order_no} FAILED: out of stock at {location_no}",
    })
    return logs


def scenario_fraud_check_block(order_no: str, location_no: str,
                              customer_email: str, t0: datetime) -> List[Dict]:
    """Fraud detection blocks the order."""
    trace_id = f"trace-{uuid.uuid4().hex[:12]}"
    score = random.randint(85, 99)
    logs = []

    logs.append({
        **_base(trace_id, order_no, location_no, customer_email, t0),
        "level": "INFO", "service": "order-service",
        "message": f"Received order request for {order_no}",
    })
    logs.append({
        **_base(trace_id, order_no, location_no, customer_email,
                t0 + timedelta(milliseconds=90)),
        "level": "INFO", "service": "fraud-detection-service",
        "message": f"Running fraud check for order {order_no}",
    })
    logs.append({
        **_base(trace_id, order_no, location_no, customer_email,
                t0 + timedelta(milliseconds=320)),
        "level": "WARN", "service": "fraud-detection-service",
        "message": (
            f"Fraud score {score}/100 for order {order_no} — "
            f"velocity:HIGH (5 orders in 10min), geo_mismatch:TRUE "
            f"(billing={location_no}, ip=Russia)"
        ),
    })
    logs.append({
        **_base(trace_id, order_no, location_no, customer_email,
                t0 + timedelta(milliseconds=350)),
        "level": "ERROR", "service": "order-service",
        "message": f"Order {order_no} BLOCKED by fraud detection (score={score})",
    })
    return logs


def scenario_downstream_503(order_no: str, location_no: str,
                            customer_email: str, t0: datetime) -> List[Dict]:
    """Downstream service returns 503 — cascading failure."""
    trace_id = f"trace-{uuid.uuid4().hex[:12]}"
    logs = []

    logs.append({
        **_base(trace_id, order_no, location_no, customer_email, t0),
        "level": "INFO", "service": "order-service",
        "message": f"Received order request for {order_no}",
    })
    logs.append({
        **_base(trace_id, order_no, location_no, customer_email,
                t0 + timedelta(milliseconds=110)),
        "level": "WARN", "service": "shipping-service",
        "message": "Carrier API (FedEx) returned 503 Service Unavailable",
    })
    logs.append({
        **_base(trace_id, order_no, location_no, customer_email,
                t0 + timedelta(milliseconds=150)),
        "level": "WARN", "service": "shipping-service",
        "message": "Retrying with secondary carrier (UPS)",
    })
    logs.append({
        **_base(trace_id, order_no, location_no, customer_email,
                t0 + timedelta(milliseconds=2110)),
        "level": "WARN", "service": "shipping-service",
        "message": "Carrier API (UPS) returned 503 Service Unavailable",
    })
    logs.append({
        **_base(trace_id, order_no, location_no, customer_email,
                t0 + timedelta(milliseconds=2150)),
        "level": "ERROR", "service": "shipping-service",
        "message": f"All shipping carriers unavailable for order {order_no}",
        "exception": (
            "com.example.shipping.CarrierUnavailableException: "
            "No available carriers (tried: FedEx, UPS, DHL, BlueDart)\n"
            "    at com.example.shipping.ShippingService.createShipment(ShippingService.java:84)"
        ),
    })
    logs.append({
        **_base(trace_id, order_no, location_no, customer_email,
                t0 + timedelta(milliseconds=2200)),
        "level": "WARN", "service": "order-service",
        "message": f"Order {order_no} partially complete: payment captured but shipping deferred",
    })
    return logs


def scenario_json_parse_error(order_no: str, location_no: str,
                              customer_email: str, t0: datetime) -> List[Dict]:
    """Bad request payload causing JSON parse failure."""
    trace_id = f"trace-{uuid.uuid4().hex[:12]}"
    return [{
        **_base(trace_id, order_no, location_no, customer_email,
                t0 + timedelta(milliseconds=15)),
        "level": "ERROR", "service": "order-service",
        "message": f"Failed to parse request body for order {order_no}",
        "exception": (
            "com.fasterxml.jackson.core.JsonParseException: Unexpected character "
            "(',' (code 44)): expected a value\n"
            " at [Source: (String)\"{\"items\":[,]}\"; line: 1, column: 11]\n"
            "    at com.fasterxml.jackson.core.JsonParser._reportError(JsonParser.java:2391)"
        ),
    }]


def scenario_oom(order_no: str, location_no: str,
                customer_email: str, t0: datetime) -> List[Dict]:
    """Out of memory in batch operation."""
    trace_id = f"trace-{uuid.uuid4().hex[:12]}"
    return [
        {
            **_base(trace_id, order_no, location_no, customer_email, t0),
            "level": "WARN", "service": "order-service",
            "message": "JVM heap usage 92% (gen=Old, capacity=2048M, used=1884M)",
        },
        {
            **_base(trace_id, order_no, location_no, customer_email,
                    t0 + timedelta(milliseconds=400)),
            "level": "ERROR", "service": "order-service",
            "message": f"Out of memory while processing order {order_no}",
            "exception": (
                "java.lang.OutOfMemoryError: Java heap space\n"
                "    at java.util.Arrays.copyOf(Arrays.java:3236)\n"
                "    at java.util.ArrayList.grow(ArrayList.java:265)\n"
                "    at com.example.order.OrderEnricher.enrichBatch(OrderEnricher.java:74)"
            ),
        },
    ]


def scenario_race_condition(order_no: str, location_no: str,
                            customer_email: str, t0: datetime) -> List[Dict]:
    """Concurrent update race condition."""
    trace_id = f"trace-{uuid.uuid4().hex[:12]}"
    return [
        {
            **_base(trace_id, order_no, location_no, customer_email, t0),
            "level": "INFO", "service": "order-service",
            "message": f"Updating order {order_no} status",
        },
        {
            **_base(trace_id, order_no, location_no, customer_email,
                    t0 + timedelta(milliseconds=85)),
            "level": "ERROR", "service": "order-service",
            "message": f"Optimistic locking failure on order {order_no}",
            "exception": (
                "org.springframework.orm.ObjectOptimisticLockingFailureException: "
                f"Row was updated or deleted by another transaction (or unsaved-value mapping was incorrect): "
                f"[com.example.order.Order#{order_no}]\n"
                "    at org.springframework.orm.jpa.vendor.HibernateJpaDialect.convertHibernateAccessException\n"
                "    at com.example.order.OrderRepository.update(OrderRepository.java:103)"
            ),
        },
    ]


# Map scenarios to weighted choice
SCENARIOS = [
    (scenario_happy_path,             50),   # 50% happy path
    (scenario_payment_gateway_timeout, 10),
    (scenario_db_connection_failure,   8),
    (scenario_inventory_mismatch,      8),
    (scenario_null_pointer,            6),
    (scenario_fraud_check_block,       6),
    (scenario_downstream_503,          5),
    (scenario_race_condition,          3),
    (scenario_oom,                     2),
    (scenario_json_parse_error,        2),
]


def pick_scenario():
    """Weighted random pick of a scenario function."""
    funcs, weights = zip(*SCENARIOS)
    return random.choices(funcs, weights=weights, k=1)[0]


# =============================================================================
# MAIN PRODUCER LOOP
# =============================================================================
async def produce_transactions(producer: AIOKafkaProducer,
                              num_transactions: int,
                              rate_logs_per_sec: float):
    """Generate transactions and send their logs to Kafka."""
    total_logs = 0

    for i in range(num_transactions):
        # Pick a random order from the seeded range
        order_id = random.randint(*ORDER_RANGE)
        order_no = f"ORD-{order_id:05d}"
        location_no = random.choice(LOCATIONS)
        customer_email = f"customer{order_id}@example.com"

        # Slightly back-dated timestamp (between now and 5 min ago)
        t0 = datetime.now(timezone.utc) - timedelta(seconds=random.randint(0, 300))

        # Generate the scenario logs
        scenario_fn = pick_scenario()
        logs = scenario_fn(order_no, location_no, customer_email, t0)

        # Publish each log to Kafka
        for log in logs:
            payload = json.dumps(log).encode("utf-8")
            # Partition by order_no so all logs for one order go to same partition
            await producer.send(KAFKA_TOPIC, value=payload, key=order_no.encode())
            total_logs += 1

            # Throttle to target rate
            if rate_logs_per_sec > 0:
                await asyncio.sleep(1.0 / rate_logs_per_sec)

        if i % 10 == 0:
            scenario_name = scenario_fn.__name__.replace("scenario_", "")
            print(f"[{i+1}/{num_transactions}] {order_no} @ {location_no} → {scenario_name} ({len(logs)} logs)")

    print(f"\n✅ Done. Total logs published: {total_logs}")


async def main():
    parser = argparse.ArgumentParser(description="Mock log generator for GenAI POC")
    parser.add_argument("--transactions", type=int, default=100,
                       help="Number of transactions to generate (default: 100)")
    parser.add_argument("--rate", type=float, default=5.0,
                       help="Logs per second (default: 5)")
    parser.add_argument("--burst", action="store_true",
                       help="Burst mode: send as fast as possible, ignore rate")
    parser.add_argument("--continuous", action="store_true",
                       help="Run continuously (Ctrl+C to stop)")
    parser.add_argument("--bootstrap", type=str, default=KAFKA_BOOTSTRAP,
                       help="Kafka bootstrap servers")
    args = parser.parse_args()

    rate = 0 if args.burst else args.rate

    producer = AIOKafkaProducer(
        bootstrap_servers=args.bootstrap,
        compression_type="gzip",
    )
    await producer.start()
    print(f"📤 Connected to Kafka at {args.bootstrap}")
    print(f"   Topic: {KAFKA_TOPIC}")
    print(f"   Rate:  {'BURST' if args.burst else f'{args.rate} logs/sec'}\n")

    try:
        if args.continuous:
            print("🔁 Continuous mode. Ctrl+C to stop.\n")
            iteration = 0
            while True:
                iteration += 1
                print(f"=== Batch {iteration} ===")
                await produce_transactions(producer, args.transactions, rate)
                await asyncio.sleep(2)
        else:
            await produce_transactions(producer, args.transactions, rate)
    finally:
        await producer.stop()
        print("📴 Producer closed.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 Stopped by user.")
