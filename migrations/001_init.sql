CREATE TABLE subscriptions (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  url text NOT NULL,
  event_types text[] NOT NULL,
  secret text NOT NULL,
  active boolean NOT NULL DEFAULT true,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE events (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  type text NOT NULL,
  payload jsonb NOT NULL,
  idempotency_key text UNIQUE,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TYPE delivery_status AS ENUM ('pending','in_flight','succeeded','dead');
CREATE TABLE deliveries (
  id bigserial PRIMARY KEY,
  event_id uuid NOT NULL REFERENCES events(id),
  subscription_id uuid NOT NULL REFERENCES subscriptions(id),
  status delivery_status NOT NULL DEFAULT 'pending',
  attempt_count int NOT NULL DEFAULT 0,
  next_attempt_at timestamptz NOT NULL DEFAULT now(),
  lease_token uuid,
  lease_expires_at timestamptz,
  last_error text,
  succeeded_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (event_id, subscription_id)
);
CREATE INDEX deliveries_due ON deliveries (next_attempt_at) WHERE status = 'pending';
CREATE INDEX deliveries_leased ON deliveries (lease_expires_at) WHERE status = 'in_flight';
CREATE TABLE delivery_attempts (
  id bigserial PRIMARY KEY,
  delivery_id bigint NOT NULL REFERENCES deliveries(id),
  attempt_no int NOT NULL,
  started_at timestamptz NOT NULL,
  finished_at timestamptz NOT NULL,
  http_status int,
  error text,
  outcome text NOT NULL,
  worker_id text NOT NULL
);
