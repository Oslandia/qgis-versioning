--
-- PostgreSQL database dump
--

SET statement_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET client_min_messages = warning;

--
-- Name: epanet; Type: SCHEMA; Schema: -
--

CREATE SCHEMA epanet;


--
-- Name: epanet_trunk_rev_head; Type: SCHEMA; Schema: -
--

CREATE SCHEMA epanet_trunk_rev_head;



--
-- Name: plpgsql; Type: EXTENSION; Schema: -; Owner: 
--

CREATE EXTENSION IF NOT EXISTS plpgsql WITH SCHEMA pg_catalog;


--
-- Name: EXTENSION plpgsql; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION plpgsql IS 'PL/pgSQL procedural language';


--
-- Name: postgis; Type: EXTENSION; Schema: -; Owner: 
--

CREATE EXTENSION IF NOT EXISTS postgis WITH SCHEMA public;


--
-- Name: EXTENSION postgis; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION postgis IS 'PostGIS geometry, geography, and raster spatial types and functions';


SET search_path = epanet, pg_catalog;

SET default_tablespace = '';

SET default_with_oids = false;

--
-- Name: junctions; Type: TABLE; Schema: epanet; Tablespace: 
--

CREATE TABLE junctions (
    id character varying,
    elevation double precision,
    base_demand_flow double precision,
    demand_pattern_id character varying,
    geom public.geometry(Point,2154),
    hid integer NOT NULL,
    trunk_rev_begin integer,
    trunk_rev_end integer,
    trunk_parent integer,
    trunk_child integer
);


--
-- Name: junctions_hid_seq; Type: SEQUENCE; Schema: epanet
--

CREATE SEQUENCE junctions_hid_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: junctions_hid_seq; Type: SEQUENCE OWNED BY; Schema: epanet
--

ALTER SEQUENCE junctions_hid_seq OWNED BY junctions.hid;


--
-- Name: pipes; Type: TABLE; Schema: epanet; Tablespace: 
--

CREATE TABLE pipes (
    id character varying,
    start_node character varying,
    end_node character varying,
    length double precision,
    diameter double precision,
    roughness double precision,
    minor_loss_coefficient double precision,
    status character varying,
    GEOMETRY public.geometry(LineString,2154),
    hid integer NOT NULL,
    trunk_rev_begin integer,
    trunk_rev_end integer,
    trunk_parent integer,
    trunk_child integer
);


--
-- Name: pipes_hid_seq; Type: SEQUENCE; Schema: epanet
--

CREATE SEQUENCE pipes_hid_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: pipes_hid_seq; Type: SEQUENCE OWNED BY; Schema: epanet
--

ALTER SEQUENCE pipes_hid_seq OWNED BY pipes.hid;


--
-- Name: revisions; Type: TABLE; Schema: epanet; Tablespace: 
--

CREATE TABLE revisions (
    rev integer NOT NULL,
    commit_msg character varying,
    branch character varying DEFAULT 'trunk'::character varying,
    date timestamp without time zone DEFAULT now(),
    author character varying
);


--
-- Name: revisions_rev_seq; Type: SEQUENCE; Schema: epanet
--

CREATE SEQUENCE revisions_rev_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: revisions_rev_seq; Type: SEQUENCE OWNED BY; Schema: epanet
--

ALTER SEQUENCE revisions_rev_seq OWNED BY revisions.rev;


SET search_path = epanet_trunk_rev_head, pg_catalog;

--
-- Name: junctions; Type: VIEW; Schema: epanet_trunk_rev_head
--

CREATE VIEW junctions AS
    SELECT junctions.hid, junctions.id, junctions.elevation, junctions.base_demand_flow, junctions.demand_pattern_id, junctions.geom FROM epanet.junctions WHERE ((junctions.trunk_rev_end IS NULL) AND (junctions.trunk_rev_begin IS NOT NULL));


--
-- Name: pipes; Type: VIEW; Schema: epanet_trunk_rev_head
--

CREATE VIEW pipes AS
    SELECT pipes.hid, pipes.id, pipes.start_node, pipes.end_node, pipes.length, pipes.diameter, pipes.roughness, pipes.minor_loss_coefficient, pipes.status, pipes.GEOMETRY FROM epanet.pipes WHERE ((pipes.trunk_rev_end IS NULL) AND (pipes.trunk_rev_begin IS NOT NULL));


SET search_path = epanet, pg_catalog;

--
-- Name: hid; Type: DEFAULT; Schema: epanet
--

ALTER TABLE ONLY junctions ALTER COLUMN hid SET DEFAULT nextval('junctions_hid_seq'::regclass);


--
-- Name: hid; Type: DEFAULT; Schema: epanet
--

ALTER TABLE ONLY pipes ALTER COLUMN hid SET DEFAULT nextval('pipes_hid_seq'::regclass);


--
-- Name: rev; Type: DEFAULT; Schema: epanet
--

ALTER TABLE ONLY revisions ALTER COLUMN rev SET DEFAULT nextval('revisions_rev_seq'::regclass);


--
-- Data for Name: junctions; Type: TABLE DATA; Schema: epanet
--

COPY junctions (id, elevation, base_demand_flow, demand_pattern_id, geom, hid, trunk_rev_begin, trunk_rev_end, trunk_parent, trunk_child) FROM stdin;
0	0	\N	\N	01010000206A080000000000000000F03F0000000000000000	1	1	\N	\N	\N
1	1	\N	\N	01010000206A0800000000000000000000000000000000F03F	2	1	\N	\N	\N
\.


--
-- Name: junctions_hid_seq; Type: SEQUENCE SET; Schema: epanet
--

SELECT pg_catalog.setval('junctions_hid_seq', 2, true);


--
-- Data for Name: pipes; Type: TABLE DATA; Schema: epanet
--

COPY pipes (id, start_node, end_node, length, diameter, roughness, minor_loss_coefficient, status, GEOMETRY, hid, trunk_rev_begin, trunk_rev_end, trunk_parent, trunk_child) FROM stdin;
4	2	3	\N	\N	\N	\N	\N	01020000206A08000002000000B411C210B508D9BF1B0E49744D1DE53F9C84E785AEE3E53F4EA96C73A15FDDBF	4	3	\N	\N	\N
0	0	1	1	2	\N	\N	\N	01020000206A08000002000000BC139FE342F9DC3F56F191C62EFEE3BF2276308E5E83E1BF541DDC72A203D83F	5	3	\N	1	\N
0	0	1	1	2	\N	\N	\N	01020000206A08000002000000F8F3853CA84AF83FA6A0C22CC87CD83FF0E70B795095E03F2AA8300B321FF63F	3	2	2	1	5
0	0	1	1	2	\N	\N	\N	01020000206A08000002000000000000000000F03F00000000000000000000000000000000000000000000F03F	1	1	2	\N	\N
1	3	3	\N	\N	\N	\N	\N	01020000206A08000002000000C28885E799B3E0BFE946DBC885D5E83FB4CB7990B55AE63F28A5CE11B68FE2BF	2	2	3	\N	\N
\.


--
-- Name: pipes_hid_seq; Type: SEQUENCE SET; Schema: epanet
--

SELECT pg_catalog.setval('pipes_hid_seq', 5, true);


--
-- Data for Name: revisions; Type: TABLE DATA; Schema: epanet
--

COPY revisions (rev, commit_msg, branch, date, author) FROM stdin;
1	initial commit	trunk	2014-01-22 16:00:44.707214	\N
2	test	trunk	2014-01-22 16:20:28.420197	vmo
3	test	trunk	2014-01-22 16:21:28.685928	vmo
4	test	trunk	2014-01-22 16:22:42.132123	vmo
\.


--
-- Name: revisions_rev_seq; Type: SEQUENCE SET; Schema: epanet
--

SELECT pg_catalog.setval('revisions_rev_seq', 1, false);


SET search_path = public, pg_catalog;

--
-- Data for Name: spatial_ref_sys; Type: TABLE DATA; Schema: public
--

COPY spatial_ref_sys (srid, auth_name, auth_srid, srtext, proj4text) FROM stdin;
\.


SET search_path = epanet, pg_catalog;

--
-- Name: junctions_pkey; Type: CONSTRAINT; Schema: epanet; Tablespace: 
--

ALTER TABLE ONLY junctions
    ADD CONSTRAINT junctions_pkey PRIMARY KEY (hid);


--
-- Name: pipes_pkey; Type: CONSTRAINT; Schema: epanet; Tablespace: 
--

ALTER TABLE ONLY pipes
    ADD CONSTRAINT pipes_pkey PRIMARY KEY (hid);


--
-- Name: revisions_pkey; Type: CONSTRAINT; Schema: epanet; Tablespace: 
--

ALTER TABLE ONLY revisions
    ADD CONSTRAINT revisions_pkey PRIMARY KEY (rev);


--
-- Name: junctions_trunk_child_fkey; Type: FK CONSTRAINT; Schema: epanet
--

ALTER TABLE ONLY junctions
    ADD CONSTRAINT junctions_trunk_child_fkey FOREIGN KEY (trunk_child) REFERENCES junctions(hid);


--
-- Name: junctions_trunk_parent_fkey; Type: FK CONSTRAINT; Schema: epanet
--

ALTER TABLE ONLY junctions
    ADD CONSTRAINT junctions_trunk_parent_fkey FOREIGN KEY (trunk_parent) REFERENCES junctions(hid);


--
-- Name: junctions_trunk_rev_begin_fkey; Type: FK CONSTRAINT; Schema: epanet
--

ALTER TABLE ONLY junctions
    ADD CONSTRAINT junctions_trunk_rev_begin_fkey FOREIGN KEY (trunk_rev_begin) REFERENCES revisions(rev);


--
-- Name: junctions_trunk_rev_end_fkey; Type: FK CONSTRAINT; Schema: epanet
--

ALTER TABLE ONLY junctions
    ADD CONSTRAINT junctions_trunk_rev_end_fkey FOREIGN KEY (trunk_rev_end) REFERENCES revisions(rev);


--
-- Name: pipes_trunk_child_fkey; Type: FK CONSTRAINT; Schema: epanet
--

ALTER TABLE ONLY pipes
    ADD CONSTRAINT pipes_trunk_child_fkey FOREIGN KEY (trunk_child) REFERENCES pipes(hid);


--
-- Name: pipes_trunk_parent_fkey; Type: FK CONSTRAINT; Schema: epanet
--

ALTER TABLE ONLY pipes
    ADD CONSTRAINT pipes_trunk_parent_fkey FOREIGN KEY (trunk_parent) REFERENCES pipes(hid);


--
-- Name: pipes_trunk_rev_begin_fkey; Type: FK CONSTRAINT; Schema: epanet
--

ALTER TABLE ONLY pipes
    ADD CONSTRAINT pipes_trunk_rev_begin_fkey FOREIGN KEY (trunk_rev_begin) REFERENCES revisions(rev);


--
-- Name: pipes_trunk_rev_end_fkey; Type: FK CONSTRAINT; Schema: epanet
--

ALTER TABLE ONLY pipes
    ADD CONSTRAINT pipes_trunk_rev_end_fkey FOREIGN KEY (trunk_rev_end) REFERENCES revisions(rev);


--
-- Name: public; Type: ACL; Schema: -; Owner: postgres
--

REVOKE ALL ON SCHEMA public FROM PUBLIC;
REVOKE ALL ON SCHEMA public FROM postgres;
GRANT ALL ON SCHEMA public TO postgres;
GRANT ALL ON SCHEMA public TO PUBLIC;


--
-- PostgreSQL database dump complete
--

