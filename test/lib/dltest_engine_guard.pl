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
exit $? >> 8;
