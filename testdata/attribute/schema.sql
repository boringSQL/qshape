CREATE TABLE accounts (
    id     int PRIMARY KEY,
    status varchar(20)
);
CREATE TABLE events (
    id         bigint PRIMARY KEY,
    account_id int,
    kind       text,
    status     varchar(20),
    email      text,
    data       jsonb,
    created_at timestamptz
);
