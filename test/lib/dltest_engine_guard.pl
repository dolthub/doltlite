#!/usr/bin/perl
# Stands in for the engine so a suite cannot read a session that died of a
# signal as a pass: the output it printed first still matches. The real
# engine runs as a child; a death by signal is appended to the log the suite
# runner checks, and the status is passed through. A caller's timeout signal
# is forwarded, so the child is not left running when the suite moves on.
# No modules: this runs once per engine session, so startup cost matters.
my $real = $ENV{DLTEST_REAL_DOLTLITE}
  or die "dltest_engine_guard: DLTEST_REAL_DOLTLITE is not set\n";
my $pid = fork();
die "dltest_engine_guard: fork failed: $!\n" unless defined $pid;
if( $pid==0 ){
  exec { $real } $real, @ARGV or exit 127;
}
$SIG{$_} = sub { kill $_[0], $pid } for qw(ALRM TERM INT HUP);
# A forwarded signal interrupts the wait while the child is still alive.
1 while waitpid($pid, 0)==-1 && kill(0, $pid);
my $signal = $? & 127;
if( $signal ){
  my $log = $ENV{DLTEST_ENGINE_SIGNAL_LOG};
  if( $log && open(my $fh, '>>', $log) ){
    print $fh "signal $signal: $real @ARGV\n";
    close $fh;
  }
  exit 128 + $signal;
}
my $rc = $? >> 8;
my $db;
for(my $i=0; $i<@ARGV; $i++){
  my $arg = $ARGV[$i];
  if( $arg eq '--' ){
    $db = $ARGV[$i+1];
    last;
  }
  if( $arg =~ /^--?(?:pagecache|lookaside)$/ ){
    $i += 2;
    next;
  }
  if( $arg =~ /^--?(?:cmd|init|separator|newline|nullvalue|vfs|heap|nonce|mmap|maxsize|screenwidth|sorterref|threadsafe|cmdline-edit|escape)$/ ){
    $i++;
    next;
  }
  next if $arg =~ /^-/;
  $db = $arg;
  last;
}
if( !defined($db) || $db eq '' || $db eq ':memory:' ){
  exit $rc;
}
my $file = $db;
if( $file =~ /^file:(.*)$/ ){
  $file = $1;
  exit $rc if $file =~ /[?&]mode=memory(?:&|$)/;
  $file =~ s/\?.*$//;
  $file =~ s/%([0-9a-fA-F]{2})/chr(hex($1))/eg;
}
$file =~ s{/$}{} if length($file)>1;
while( !-f $file && $file =~ s{/[^/]+$}{} ){}
exit $rc unless -f $file;
my $expected;
my $exceptions = $ENV{DLTEST_INTEGRITY_EXPECTATIONS};
if( $exceptions && open(my $fh, '<', $exceptions) ){
  while(my $line = <$fh>){
    chomp $line;
    my ($path, $pattern) = split(/\t/, $line, 2);
    $expected = $pattern if defined($pattern) && ($path eq $db || $path eq $file);
  }
  close $fh;
}
exit $rc if $expected && $expected =~ /^skip:.+/;
my $probe = $db;
$probe =~ s/([?&]mode=)rwc?(?=&|$)/${1}ro/ if $probe =~ /^file:/;
CHECK:
pipe(my $reader, my $writer) or die "integrity pipe failed: $!\n";
$pid = fork();
die "integrity fork failed: $!\n" unless defined $pid;
if( $pid==0 ){
  close $reader;
  open(STDIN, '<', '/dev/null') or exit 127;
  open(STDOUT, '>&', $writer) or exit 127;
  open(STDERR, '>&', $writer) or exit 127;
  close $writer;
  exec { $real } $real, '-readonly', '-bail', '-batch', '-noheader',
      '-list', '-init', '/dev/null', $probe, 'PRAGMA integrity_check;' or exit 127;
}
close $writer;
my $result = do { local $/; <$reader> };
close $reader;
1 while waitpid($pid, 0)==-1 && kill(0, $pid);
my $status = $?;
$result = '' unless defined $result;
$result =~ s/\r//g;
$result =~ s/\n$//;
if( $status && $probe ne $file && $result =~ /branch or revision .* not found/ ){
  $probe = $file;
  goto CHECK;
}
my $ok = $status==0 && $result eq 'ok';
if( !$ok && $status==0 && $expected ){
  $ok = length($result)>0;
  for my $line (split(/\n/, $result)){
    $ok = 0 unless $line =~ /$expected/;
  }
}
if( !$ok ){
  my $message = "integrity failure: $db (status $status): $result\n";
  print STDERR $message;
  my $log = $ENV{DLTEST_ENGINE_INTEGRITY_LOG};
  if( $log && open(my $fh, '>>', $log) ){
    print $fh $message;
    close $fh;
  }
  exit 125;
}
exit $rc;
