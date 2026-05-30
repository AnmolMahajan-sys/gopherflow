CREATE TYPE workflow_status AS ENUM ('PENDING','RUNNING','COMPLETED','FAILED');
CREATE TYPE stage_status AS ENUM ('PENDING','READY','RUNNING','COMPLETED','FAILED');

CREATE TABLE workflows(
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(255) NOT NULL,
    status workflow_status NOT NULL DEFAULT 'PENDING',
    created_at TIMESTAMP NOT NULL DEFAULT now(),
    updated_at TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE stages(
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workflow_id     UUID NOT NULL REFERENCES workflows(id) ON DELETE CASCADE,
    name            VARCHAR(255) NOT NULL,
    status          stage_status NOT NULL DEFAULT 'PENDING',
    depends_on      UUID[],
    payload         JSONB,
    created_at      TIMESTAMP NOT NULL DEFAULT now(),
    updated_at      TIMESTAMP NOT NULL DEFAULT now()
);

CREATE INDEX idx_stages_workflow_id ON stages(workflow_id);
CREATE INDEX idx_stages_status ON stages(status);