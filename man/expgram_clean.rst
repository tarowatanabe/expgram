=============
expgram_clean
=============

-----------------------------
clear temporary created files
-----------------------------

:Author: Taro Watanabe <taro.watanabe@nict.go.jp>
:Date:   2013-7-25
:Manual section: 1

SYNOPSIS
--------

**expgram_clean** [*options*]

DESCRIPTION
-----------

`expgram` write temporary data at temporary directory specified either
by TMPDIR or TMPDIR_SPEC environment variables, or the --temporary (or
--temporary-dir) program option. All the applications can detect and
clear unused files when exit, but occasionally, this may be impossible
due by unexpected segfaults. `expgram_clean` tries to clean up those
temporary data.

OPTIONS
-------

  **--temporary** `arg`        temporary directory

  **--help** help message

ENVIRONMENT
-----------

TMPDIR
  Temporary directory.

TMPDIR_SPEC
  An alternative temporary directory. If **TMPDIR_SPEC** is specified,
  this is preferred over **TMPDIR**. In addition, if
  **--temporary** is specified, program option is preferred over
  environment variables.

  The temporary directory specified either by **TMPDIR_SPEC** or by
  **--temporary** has a special treatment in that the keyword
  %host is replaced by the host of running machine. For instance, you
  can set:

    /temporary/%host/tmp

  and your running machine is run005, then, the temporary directory
  will be /temporary/run005/tmp.

SEE ALSO
--------

`expgram.py(1)`
