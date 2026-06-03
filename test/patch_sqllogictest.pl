#!/usr/bin/perl
# Patch sqllogictest.c to emit one stable "!DIVERGE <query-start-line>" token
# per query record that produced any verify mismatch. The harness pairs these
# with the per-file path it is running to build/check the known-divergence list.
use strict; use warnings;
local $/; my $s = <STDIN>;

# 1) Declare a per-query error snapshot + the query's start line.
my $anchor1 = "    }else if( strcmp(sScript.azToken[0],\"query\")==0 ){\n      int k = 0;\n      int c;\n";
my $repl1   = $anchor1 . "      int dErr0 = nErr;\n      int qDivLine = sScript.startLine;\n";
($s =~ s/\Q$anchor1\E/$repl1/) or die "anchor1 (query branch head) not found\n";

# 2) query-failed path: emit before the continue.
my $anchor2 = "        pEngine->xFreeResults(pConn, azResult, nResult);\n        nErr++;\n        continue;\n";
my $repl2   = "        pEngine->xFreeResults(pConn, azResult, nResult);\n        nErr++;\n        fprintf(stdout, \"!DIVERGE %d\\n\", qDivLine);\n        continue;\n";
($s =~ s/\Q$anchor2\E/$repl2/) or die "anchor2 (query failed path) not found\n";

# 3) normal end of query record: emit if any mismatch happened.
my $anchor3 = "      /* Free the query results */\n      pEngine->xFreeResults(pConn, azResult, nResult);\n";
my $repl3   = $anchor3 . "      if( nErr > dErr0 ) fprintf(stdout, \"!DIVERGE %d\\n\", qDivLine);\n";
($s =~ s/\Q$anchor3\E/$repl3/) or die "anchor3 (free query results) not found\n";

print $s;
