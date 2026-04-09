BEGIN;
CREATE TABLE symbol(
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL,
  isTerminal BOOLEAN NOT NULL,
  fallback INTEGER REFERENCES symbol DEFERRABLE INITIALLY DEFERRED
);
INSERT INTO symbol(id,name,isTerminal,fallback)VALUES(0,'$',TRUE,NULL);
INSERT INTO symbol(id,name,isTerminal,fallback)VALUES(1,'OR',TRUE,NULL);
INSERT INTO symbol(id,name,isTerminal,fallback)VALUES(2,'AND',TRUE,NULL);
INSERT INTO symbol(id,name,isTerminal,fallback)VALUES(3,'NOT',TRUE,NULL);
INSERT INTO symbol(id,name,isTerminal,fallback)VALUES(4,'TERM',TRUE,NULL);
INSERT INTO symbol(id,name,isTerminal,fallback)VALUES(5,'COLON',TRUE,NULL);
INSERT INTO symbol(id,name,isTerminal,fallback)VALUES(6,'MINUS',TRUE,NULL);
INSERT INTO symbol(id,name,isTerminal,fallback)VALUES(7,'LCP',TRUE,NULL);
INSERT INTO symbol(id,name,isTerminal,fallback)VALUES(8,'RCP',TRUE,NULL);
INSERT INTO symbol(id,name,isTerminal,fallback)VALUES(9,'STRING',TRUE,NULL);
INSERT INTO symbol(id,name,isTerminal,fallback)VALUES(10,'LP',TRUE,NULL);
INSERT INTO symbol(id,name,isTerminal,fallback)VALUES(11,'RP',TRUE,NULL);
INSERT INTO symbol(id,name,isTerminal,fallback)VALUES(12,'CARET',TRUE,NULL);
INSERT INTO symbol(id,name,isTerminal,fallback)VALUES(13,'COMMA',TRUE,NULL);
INSERT INTO symbol(id,name,isTerminal,fallback)VALUES(14,'PLUS',TRUE,NULL);
INSERT INTO symbol(id,name,isTerminal,fallback)VALUES(15,'STAR',TRUE,NULL);
INSERT INTO symbol(id,name,isTerminal,fallback)VALUES(16,'input',FALSE,NULL);
INSERT INTO symbol(id,name,isTerminal,fallback)VALUES(17,'expr',FALSE,NULL);
INSERT INTO symbol(id,name,isTerminal,fallback)VALUES(18,'cnearset',FALSE,NULL);
INSERT INTO symbol(id,name,isTerminal,fallback)VALUES(19,'exprlist',FALSE,NULL);
INSERT INTO symbol(id,name,isTerminal,fallback)VALUES(20,'colset',FALSE,NULL);
INSERT INTO symbol(id,name,isTerminal,fallback)VALUES(21,'colsetlist',FALSE,NULL);
INSERT INTO symbol(id,name,isTerminal,fallback)VALUES(22,'nearset',FALSE,NULL);
INSERT INTO symbol(id,name,isTerminal,fallback)VALUES(23,'nearphrases',FALSE,NULL);
INSERT INTO symbol(id,name,isTerminal,fallback)VALUES(24,'phrase',FALSE,NULL);
INSERT INTO symbol(id,name,isTerminal,fallback)VALUES(25,'neardist_opt',FALSE,NULL);
INSERT INTO symbol(id,name,isTerminal,fallback)VALUES(26,'star_opt',FALSE,NULL);
CREATE TABLE rule(
  ruleid INTEGER PRIMARY KEY,
  lhs INTEGER REFERENCES symbol(id),
  txt TEXT
);
CREATE TABLE rulerhs(
  ruleid INTEGER REFERENCES rule(ruleid),
  pos INTEGER,
  sym INTEGER REFERENCES symbol(id)
);
INSERT INTO rule(ruleid,lhs,txt)VALUES(0,16,'input ::= expr');
INSERT INTO rulerhs(ruleid,pos,sym)VALUES(0,0,17);
INSERT INTO rule(ruleid,lhs,txt)VALUES(1,20,'colset ::= MINUS LCP colsetlist RCP');
INSERT INTO rulerhs(ruleid,pos,sym)VALUES(1,0,6);
INSERT INTO rulerhs(ruleid,pos,sym)VALUES(1,1,7);
INSERT INTO rulerhs(ruleid,pos,sym)VALUES(1,2,21);
INSERT INTO rulerhs(ruleid,pos,sym)VALUES(1,3,8);
INSERT INTO rule(ruleid,lhs,txt)VALUES(2,20,'colset ::= LCP colsetlist RCP');
INSERT INTO rulerhs(ruleid,pos,sym)VALUES(2,0,7);
INSERT INTO rulerhs(ruleid,pos,sym)VALUES(2,1,21);
INSERT INTO rulerhs(ruleid,pos,sym)VALUES(2,2,8);
INSERT INTO rule(ruleid,lhs,txt)VALUES(3,20,'colset ::= STRING');
INSERT INTO rulerhs(ruleid,pos,sym)VALUES(3,0,9);
INSERT INTO rule(ruleid,lhs,txt)VALUES(4,20,'colset ::= MINUS STRING');
INSERT INTO rulerhs(ruleid,pos,sym)VALUES(4,0,6);
INSERT INTO rulerhs(ruleid,pos,sym)VALUES(4,1,9);
INSERT INTO rule(ruleid,lhs,txt)VALUES(5,21,'colsetlist ::= colsetlist STRING');
INSERT INTO rulerhs(ruleid,pos,sym)VALUES(5,0,21);
INSERT INTO rulerhs(ruleid,pos,sym)VALUES(5,1,9);
INSERT INTO rule(ruleid,lhs,txt)VALUES(6,21,'colsetlist ::= STRING');
INSERT INTO rulerhs(ruleid,pos,sym)VALUES(6,0,9);
INSERT INTO rule(ruleid,lhs,txt)VALUES(7,17,'expr ::= expr AND expr');
INSERT INTO rulerhs(ruleid,pos,sym)VALUES(7,0,17);
INSERT INTO rulerhs(ruleid,pos,sym)VALUES(7,1,2);
INSERT INTO rulerhs(ruleid,pos,sym)VALUES(7,2,17);
INSERT INTO rule(ruleid,lhs,txt)VALUES(8,17,'expr ::= expr OR expr');
INSERT INTO rulerhs(ruleid,pos,sym)VALUES(8,0,17);
INSERT INTO rulerhs(ruleid,pos,sym)VALUES(8,1,1);
INSERT INTO rulerhs(ruleid,pos,sym)VALUES(8,2,17);
INSERT INTO rule(ruleid,lhs,txt)VALUES(9,17,'expr ::= expr NOT expr');
INSERT INTO rulerhs(ruleid,pos,sym)VALUES(9,0,17);
INSERT INTO rulerhs(ruleid,pos,sym)VALUES(9,1,3);
INSERT INTO rulerhs(ruleid,pos,sym)VALUES(9,2,17);
INSERT INTO rule(ruleid,lhs,txt)VALUES(10,17,'expr ::= colset COLON LP expr RP');
INSERT INTO rulerhs(ruleid,pos,sym)VALUES(10,0,20);
INSERT INTO rulerhs(ruleid,pos,sym)VALUES(10,1,5);
INSERT INTO rulerhs(ruleid,pos,sym)VALUES(10,2,10);
INSERT INTO rulerhs(ruleid,pos,sym)VALUES(10,3,17);
INSERT INTO rulerhs(ruleid,pos,sym)VALUES(10,4,11);
INSERT INTO rule(ruleid,lhs,txt)VALUES(11,17,'expr ::= LP expr RP');
INSERT INTO rulerhs(ruleid,pos,sym)VALUES(11,0,10);
INSERT INTO rulerhs(ruleid,pos,sym)VALUES(11,1,17);
INSERT INTO rulerhs(ruleid,pos,sym)VALUES(11,2,11);
INSERT INTO rule(ruleid,lhs,txt)VALUES(12,17,'expr ::= exprlist');
INSERT INTO rulerhs(ruleid,pos,sym)VALUES(12,0,19);
INSERT INTO rule(ruleid,lhs,txt)VALUES(13,19,'exprlist ::= cnearset');
INSERT INTO rulerhs(ruleid,pos,sym)VALUES(13,0,18);
INSERT INTO rule(ruleid,lhs,txt)VALUES(14,19,'exprlist ::= exprlist cnearset');
INSERT INTO rulerhs(ruleid,pos,sym)VALUES(14,0,19);
INSERT INTO rulerhs(ruleid,pos,sym)VALUES(14,1,18);
INSERT INTO rule(ruleid,lhs,txt)VALUES(15,18,'cnearset ::= nearset');
INSERT INTO rulerhs(ruleid,pos,sym)VALUES(15,0,22);
INSERT INTO rule(ruleid,lhs,txt)VALUES(16,18,'cnearset ::= colset COLON nearset');
INSERT INTO rulerhs(ruleid,pos,sym)VALUES(16,0,20);
INSERT INTO rulerhs(ruleid,pos,sym)VALUES(16,1,5);
INSERT INTO rulerhs(ruleid,pos,sym)VALUES(16,2,22);
INSERT INTO rule(ruleid,lhs,txt)VALUES(17,22,'nearset ::= phrase');
INSERT INTO rulerhs(ruleid,pos,sym)VALUES(17,0,24);
INSERT INTO rule(ruleid,lhs,txt)VALUES(18,22,'nearset ::= CARET phrase');
INSERT INTO rulerhs(ruleid,pos,sym)VALUES(18,0,12);
INSERT INTO rulerhs(ruleid,pos,sym)VALUES(18,1,24);
INSERT INTO rule(ruleid,lhs,txt)VALUES(19,22,'nearset ::= STRING LP nearphrases neardist_opt RP');
INSERT INTO rulerhs(ruleid,pos,sym)VALUES(19,0,9);
INSERT INTO rulerhs(ruleid,pos,sym)VALUES(19,1,10);
INSERT INTO rulerhs(ruleid,pos,sym)VALUES(19,2,23);
INSERT INTO rulerhs(ruleid,pos,sym)VALUES(19,3,25);
INSERT INTO rulerhs(ruleid,pos,sym)VALUES(19,4,11);
INSERT INTO rule(ruleid,lhs,txt)VALUES(20,23,'nearphrases ::= phrase');
INSERT INTO rulerhs(ruleid,pos,sym)VALUES(20,0,24);
INSERT INTO rule(ruleid,lhs,txt)VALUES(21,23,'nearphrases ::= nearphrases phrase');
INSERT INTO rulerhs(ruleid,pos,sym)VALUES(21,0,23);
INSERT INTO rulerhs(ruleid,pos,sym)VALUES(21,1,24);
INSERT INTO rule(ruleid,lhs,txt)VALUES(22,25,'neardist_opt ::=');
INSERT INTO rule(ruleid,lhs,txt)VALUES(23,25,'neardist_opt ::= COMMA STRING');
INSERT INTO rulerhs(ruleid,pos,sym)VALUES(23,0,13);
INSERT INTO rulerhs(ruleid,pos,sym)VALUES(23,1,9);
INSERT INTO rule(ruleid,lhs,txt)VALUES(24,24,'phrase ::= phrase PLUS STRING star_opt');
INSERT INTO rulerhs(ruleid,pos,sym)VALUES(24,0,24);
INSERT INTO rulerhs(ruleid,pos,sym)VALUES(24,1,14);
INSERT INTO rulerhs(ruleid,pos,sym)VALUES(24,2,9);
INSERT INTO rulerhs(ruleid,pos,sym)VALUES(24,3,26);
INSERT INTO rule(ruleid,lhs,txt)VALUES(25,24,'phrase ::= STRING star_opt');
INSERT INTO rulerhs(ruleid,pos,sym)VALUES(25,0,9);
INSERT INTO rulerhs(ruleid,pos,sym)VALUES(25,1,26);
INSERT INTO rule(ruleid,lhs,txt)VALUES(26,26,'star_opt ::= STAR');
INSERT INTO rulerhs(ruleid,pos,sym)VALUES(26,0,15);
INSERT INTO rule(ruleid,lhs,txt)VALUES(27,26,'star_opt ::=');
COMMIT;
