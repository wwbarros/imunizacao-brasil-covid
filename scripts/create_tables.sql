CREATE TABLE vaccines
(
  id int not null,
  name  string not null,
  supplier string not null
);
CREATE TABLE health_institution
(
  id int NOT NULL,
  name string NOT NULL
  organization string NOT NULL
  state string NOT NULL
  city string NOT NULL
);
CREATE TABLE category
(
  id int NOT NULL
  name string NOT NULL
);
CREATE TABLE population_group
(
  id string NOT NULL
  name string NOT NULL
); 
CREATE TABLE patient
(
  id string NOT NULL
  age int NOT NULL
  birthdate date NOT NULL
  gender string NOT NULL
  country string NOT NULL
  state string NOT NULL
  city string NOT NULL
);
CREATE TABLE imunization
(
  patient_id string NOT NULL
  health_institution_id int NOT NULL
  category_id int NOT NULL
  population_group_id string NOT NULL
  vaccines_id int NOT NULL
  vaccines_dose string NOT NULL
  jab_date date NOT NULL
);