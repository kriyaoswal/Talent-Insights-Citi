-- data-eng-001 "Performance & Talent Management" source, generated
-- synthetic data. Load order: teams, competencies, employees, projects,
-- performance_reviews, competency_assessments, talent_review.

CREATE TABLE teams (
    team_id         INTEGER PRIMARY KEY,
    name            TEXT NOT NULL,
    department      TEXT NOT NULL
);

CREATE TABLE competencies (
    competency_id   INTEGER PRIMARY KEY,
    name            TEXT NOT NULL,
    category        TEXT NOT NULL CHECK (category IN ('Technical', 'Leadership', 'Domain'))
);

CREATE TABLE employees (
    employee_id     INTEGER PRIMARY KEY,
    first_name      TEXT NOT NULL,
    last_name       TEXT NOT NULL,
    email           TEXT NOT NULL,
    team_id         INTEGER NOT NULL REFERENCES teams(team_id),
    job_level       TEXT NOT NULL,
    hire_date       DATE NOT NULL
);

CREATE TABLE projects (
    project_id      INTEGER PRIMARY KEY,
    name            TEXT NOT NULL,
    team_id         INTEGER NOT NULL REFERENCES teams(team_id),
    outcome_status  TEXT NOT NULL CHECK (outcome_status IN ('on-time', 'delayed', 'at-risk', 'cancelled')),
    outcome_score   NUMERIC(5,2) NOT NULL
);

CREATE TABLE performance_reviews (
    review_id       INTEGER PRIMARY KEY,
    employee_id     INTEGER NOT NULL REFERENCES employees(employee_id),
    review_period   TEXT NOT NULL,
    rating          INTEGER NOT NULL CHECK (rating BETWEEN 1 AND 5),
    manager_comment TEXT,
    submitted_date  DATE NOT NULL
);

CREATE TABLE competency_assessments (
    assessment_id   INTEGER PRIMARY KEY,
    employee_id     INTEGER NOT NULL REFERENCES employees(employee_id),
    competency_id   INTEGER NOT NULL REFERENCES competencies(competency_id),
    proficiency_level INTEGER NOT NULL CHECK (proficiency_level BETWEEN 1 AND 5),
    required_level    INTEGER NOT NULL CHECK (required_level BETWEEN 1 AND 5),
    assessed_date   DATE NOT NULL
);

CREATE TABLE talent_review (
    employee_id         INTEGER PRIMARY KEY REFERENCES employees(employee_id),
    review_cycle        TEXT NOT NULL,
    potential_rating    TEXT NOT NULL CHECK (potential_rating IN ('Low', 'Medium', 'High')),
    promotion_readiness TEXT NOT NULL CHECK (promotion_readiness IN ('Ready Now', '1-2 Years', 'Not Ready')),
    attrition_risk      TEXT NOT NULL CHECK (attrition_risk IN ('Low', 'Medium', 'High'))
);

CREATE INDEX idx_employees_team_id ON employees(team_id);
CREATE INDEX idx_reviews_employee_id ON performance_reviews(employee_id);
CREATE INDEX idx_assessments_employee_id ON competency_assessments(employee_id);
