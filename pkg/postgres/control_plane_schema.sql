-- Control plane registrations (PostgreSQL)

CREATE TABLE IF NOT EXISTS control_planes
(
    id            TEXT PRIMARY KEY NOT NULL, -- ControlPlane.ID (controlplaneId of the registration)
    endpoint      TEXT             NOT NULL, -- ControlPlane.Endpoint (base URL of the control plane signaling API)
    auth          JSONB            NOT NULL  -- ControlPlane.Authorization ("authorization" is a reserved word);
                                             -- ({"type": ..., ...profile properties})
);
