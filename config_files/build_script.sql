PRAGMA foreign_keys = ON;

create table gwas_methyl_lookup (
    id integer primary key autoincrement,
    methyl_sample_id text unique,
    gwas_sample_id text unique
);

create table methyl_probe_name_lookup (
    id integer primary key autoincrement,
    chrm text,
    bp integer,
    probe_name text unique,
    UNIQUE(chrm, bp)
);
-- table will be too large
-- create table methyl_probe (
--     id integer primary key autoincrement,
--     gwas_methyl_lookup_id integer,
--     probe_id integer,
--     beta_value float,
--     UNIQUE(gwas_methyl_lookup_id, probe_id),
--     FOREIGN KEY(gwas_methyl_lookup_id) REFERENCES gwas_methyl_lookup(id),
--     FOREIGN KEY(probe_id) REFERENCES methyl_probe_name_lookup(id)
-- );

create table snp_name_lookup (
    id integer primary key autoincrement,
    chrm text,
    bp integer,
    snp_name text unique,
    UNIQUE(chrm, bp)
);

-- table will be too large
-- create table genotype (
--     id integer primary key autoincrement,
--     gwas_methyl_lookup_id integer,
--     genotype integer,
--     snp_id integer,
--     UNIQUE(gwas_methyl_lookup_id, snp_id),
--     FOREIGN KEY(gwas_methyl_lookup_id) REFERENCES gwas_methyl_lookup(id),
--     FOREIGN KEY(snp_id) REFERENCES snp_name_lookup(id)
-- );

create table gemes (
    id integer primary key autoincrement,
    probe_id integer,
    snp_id integer,
    FOREIGN KEY (probe_id) REFERENCES methyl_probe_name_lookup(id),
    FOREIGN KEY (snp_id) REFERENCES snp_name_lookup(id),
    UNIQUE(probe_id, snp_id)
);