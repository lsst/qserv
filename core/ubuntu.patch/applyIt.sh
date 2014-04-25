#!/bin/sh

(cd modules/proto;  patch SConscript.test < ../../ubuntu.patch/proto_SConscript.test)
(cd modules/sql;    patch SConscript.test < ../../ubuntu.patch/sql_SConscript.test)
(cd modules/wdb;    patch SConscript.test < ../../ubuntu.patch/wdb_SConscript.test)
(cd modules/wsched; patch SConscript.test < ../../ubuntu.patch/wsched_SConscript.test)
(cd modules/xrdfs;  patch SConscript.test < ../../ubuntu.patch/xrdfs_SConscript.test)
(cd modules/xrdoss; patch SConscript.test < ../../ubuntu.patch/xrdoss_SConscript.test)
