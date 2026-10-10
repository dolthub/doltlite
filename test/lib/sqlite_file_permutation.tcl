namespace eval sqlite_file {
  variable mode $::env(SQLITE_FILE_TEST_MODE)
  variable opened 0
  variable attached 0
  variable active 0
}
if {$sqlite_file::mode ni {sqlite mixed}} {
  error "invalid SQLITE_FILE_TEST_MODE"
}
set suite [lindex $argv 0]
set argv [lrange $argv 1 end]
set argv0 $suite
if {$sqlite_file::mode eq "sqlite"} {
  set sqlite_options(doltlite) 0
}

rename sqlite3 sqlite_file::native
proc sqlite_file::uri {filename} {
  if {![string match "file:*" $filename]} {
    set filename "file:[string map {% %25 ? %3f # %23} $filename]"
  }
  append filename [expr {[string first ? $filename]<0 ? "?" : "&"}]
  append filename "doltlite_engine=sqlite"
  return $filename
}

proc sqlite3 {args} {
  if {[llength $args]<2 || [string index [lindex $args 0] 0] eq "-"} {
    return [uplevel 1 [list sqlite_file::native {*}$args]]
  }
  set handle [lindex $args 0]
  set filename [lindex $args 1]
  set stock [expr {$sqlite_file::mode eq "sqlite" ||
    ($filename ni {:memory: {}} && [file tail $filename] ne "test.db" &&
     ![string match "file:*" $filename])}]
  if {$stock && $filename ne ""} {
    set args [lreplace $args 1 1 [sqlite_file::uri $filename]]
    lappend args -uri 1
  }
  set res [uplevel 1 [list sqlite_file::native {*}$args]]
  if {$stock && [doltlite_test_engine $handle] ne "orig"} {
    error "SQLite permutation opened $filename on the wrong engine"
  }
  if {$stock} {incr sqlite_file::opened}
  trace add execution $handle {enter leave} sqlite_file::sql_trace
  return $res
}

proc sqlite_file::sql_trace {command args} {
  variable active
  variable mode
  variable attached
  if {$active || [lindex $command 1] ni {eval onecolumn exists}} return
  set sql [lindex $command 2]
  if {![regexp -nocase {\mATTACH\M} $sql]} return
  set active 1
  try {
    set op [lindex $args end]
    if {$op eq "enter" && $mode eq "sqlite"} {
      foreach {match filename} [regexp -all -inline -nocase -expanded {
        \mATTACH\s+(?:DATABASE\s+)?'([^']+)'\s+AS\M
      } $sql] {
        if {[regexp {^test[^/]*\.db[^/]*$} $filename] &&
            (![file exists $filename] || [file size $filename]==0)} {
          native sqlite_file_seed [uri $filename] -uri 1
          sqlite_file_seed eval {PRAGMA user_version=0}
          sqlite_file_seed close
        }
      }
    } elseif {$op eq "leave" && [lindex $args 0]==0} {
      set handle [lindex $command 0]
      set engine [doltlite_test_engine $handle]
      foreach {seq name filename} [$handle eval {PRAGMA database_list}] {
        if {$seq<2 || ![file isfile $filename] || [file size $filename]<16} continue
        set fd [open $filename rb]
        set header [read $fd 16]
        close $fd
        if {$header eq "SQLite format 3\x00" &&
            [doltlite_test_engine $handle $name] eq "orig" &&
            ($mode eq "sqlite" || $engine eq "prolly")} {
          incr attached
        }
      }
    }
  } finally {
    set active 0
  }
}

set testdir [file dirname $suite]
source $testdir/tester.tcl
proc sqlite_file::finish {command op} {
  do_test sqlite-file.engine {expr {$sqlite_file::opened>0}} 1
  if {[file tail $::suite] in {attach.test attach3.test}} {
    do_test sqlite-file.existing-attachment {expr {$sqlite_file::attached>0}} 1
  }
}
trace add execution finish_test enter sqlite_file::finish
source $suite
