set suite [lindex $argv 0]
set argv [lrange $argv 1 end]
set argv0 $suite
set sqlite_file_engine [file join [file dirname [info script]] sqlite_file_engine.tcl]
source $sqlite_file_engine
if {$sqlite_file::mode eq "sqlite"} {
  set sqlite_options(doltlite) 0
}

proc sqlite_file::child {command code result op} {
  if {$code==0} {
    testfixture $result [list source $::sqlite_file_engine]
  }
}
proc sqlite_file::source_done {command code result op} {
  if {$code==0 && [file tail [lindex $command end]] eq "lock_common.tcl"} {
    trace add execution ::launch_testfixture leave sqlite_file::child
  }
}
trace add execution source leave sqlite_file::source_done

set testdir [file dirname $suite]
source $testdir/tester.tcl
proc crashsql [info args crashsql] [string map [list \
  {puts $f "autoinstall_test_functions"} \
  {puts $f "autoinstall_test_functions"
  puts $f [list source $::sqlite_file_engine]}] [info body crashsql]]
proc sqlite_file::finish {command op} {
  do_test sqlite-file.engine {expr {$sqlite_file::opened>0}} 1
  if {[file tail $::suite] in {attach.test attach3.test}} {
    do_test sqlite-file.existing-attachment {expr {$sqlite_file::attached>0}} 1
  }
}
trace add execution finish_test enter sqlite_file::finish
source $suite
