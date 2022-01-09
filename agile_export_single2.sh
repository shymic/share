#!/bin/ksh
##################################################################################
# Author            : Thomas Kebelmann
# Created           : Jul 2015
#
# Purpose           : Starts the flatflie exports for a single entry within table META_AGILE_EXPORT_RUN
# Call              : agile_export_single.sh <SQLPATH> <SQLFILE> <EXPORTPATH> <EXPORTFILE> <COMPRESS_FLAG>
#                     SQLPATH       defines the path where the export SQL is placed
#                     SQLFILE       defines the file containing the export SQL
#                     EXPORTPATH    defines the path where the export result is to be placed
#                     EXPORTFILE    defines the file containing the export result is to contains (without .gz ending in case of compress)
#                     COMPRESS_FLAG defnies (Y/N) if the result is to be compressed with gzip after export
# 2016-12-01        : TK Major Changes!
#                     Source DB possible, generate runtime statistics
# 2017-04-02        : TK add feature: header line is allowed to start with -- as sql comment, will be removed dynamically
# 2018-05-17        : behrelu: allow special characters in db connect pw
# 2019-02-08        : behrelu/steinri: fix issue with space characters in directory names
# 2019-05-02        : kebelth: enhanced Error handling / debugging
# 2019-08-02        : klemmmi: add pre export sql exec file handling
# 2021-09-06        : gacekan: improved and fixed error handling
# 2022-01-06        : shymkan: add 'with ur' construction

. ${HOME}/hlag_env_config

# Input parameters
v_sqlpath=$1    # path of the sql
v_sqlfile=$2    # name of the sql file
v_exportpath=$3 # path of the export
if [ ! -d "$v_exportpath" ]; then mkdir -p "$v_exportpath"; fi
v_exportfile=$4 # name of the export file
v_zipfile=$5    # gzip option Y/N
v_sqlfilefull=$v_sqlpath"/"$v_sqlfile
v_exportfilefull="$v_exportpath""/"$v_exportfile
v_source_db=$6  # source DB name

# set special path, files and variables
v_source_db_cfg=/opt/hlag/agile_export/etc/source_databases.cfg # the file with all DB configurations
v_pwdecryptkey=`head -1 ~/script.key`

#set prepare export sql file
v_pre_sqlfile=$(basename $v_sqlfile .sql)_pre.sql
v_pre_sqlfilefull=$v_sqlpath"/"$v_pre_sqlfile

# temporary files
v_tmpexportpath=$PMRootDir/tmp_agile_export
v_tmpexportfile=$v_tmpexportpath"/"$v_exportfile
v_tmpexportfile2=$v_tmpexportpath"/"$v_exportfile"2"
v_tmpsqlfile="/tmp/"$v_sqlfile
v_tmpmsgfile="/tmp/exportmsg_"$v_exportfile
v_tmpsqloutput="/tmp/sqloutput_"$v_sqlfile

# connect parameters, taken from the surce configration file, construct connect string
source_db_cfg_line=`cat $v_source_db_cfg | grep ^$v_source_db`
v_schema=`echo $source_db_cfg_line | cut -d"," -f4`
v_extract_method=`echo $source_db_cfg_line | cut -d"," -f2`
ETL_DB_READ_CONNECT=`echo $source_db_cfg_line | cut -d"," -f3`
ETL_DB_READ_USER_ID=`echo $source_db_cfg_line | cut -d"," -f5`
ETL_DB_READ_USER_PW=`echo $source_db_cfg_line | cut -d"," -f6 | openssl enc -aes-128-cbc -a -d -salt -pass pass:$v_pwdecryptkey`
cnncmd="connect to "$ETL_DB_READ_CONNECT" user "$ETL_DB_READ_USER_ID" using '"$ETL_DB_READ_USER_PW"'"

# functions
echo_dt()
{
  v_msg=$1
  echo  `date '+%Y%m%d%H%M%S'`$v_msg
}

echo_fail()
{
  echo_dt " ##### Export failed! ######"
}

gzip_exp()
{
  echo_dt " - Running gzip "$v_tmpexportfile"."
  gzip "$v_tmpexportfile"
  if [ $? -gt 0 ]; then
    RC=8
    echo_dt " - ERROR - gzip failed."
    echo_fail
  else
    v_tmpexportfile=$v_tmpexportfile.gz
  fi
}

mv_exp_path()
{
  v_attempts=$1
  v_attempts_current=1
  echo_dt " - Moving $v_tmpexportfile to $v_exportpath (in $v_attempts attempt/s)."
  RCMV=1
  while [ $v_attempts -ge $v_attempts_current -a $RCMV -ne 0 ]; do
    mv -f "$v_tmpexportfile" "$v_exportpath/."
    RCMV=$?
    if [ $RCMV -gt 0 ]; then
      echo_dt " - ERROR - Move failed (attempt $v_attempts_current/$v_attempts)."
      if [ $v_attempts_current -lt $v_attempts ]; then
        sleep 60
      else
        RC=8
        echo_fail
      fi
      v_attempts_current=$((v_attempts_current+1))
    else
      echo_dt " - SUCCESS - Move completed (attempt $v_attempts_current/$v_attempts)."
    fi
  done
}

process_exception()
{
  v_file=$1
  if [ $DB2RC -eq 4 ]; then
    cat $v_file
    echo_dt " - ERROR - SQL statement error or statement interrupted."
  elif [ $DB2RC -gt 4 ]; then
    cat $v_file
    echo_dt " - ERROR - SQL Processor system error."
  elif [ $MAINRC ]; then
    echo_dt " - ERROR - Export interrupted."
  elif [ -f $v_file ]; then
    cat $v_file
  fi
  echo_fail
  RC=8
}

# main



if [ ! -f $v_sqlfilefull ]; then # does the export sql file exist?
  echo_dt " - ERROR - Cannot find SQL $v_sqlfilefull"
  echo_fail
  exit 8
fi

if ! grep -i 'with ur' $v_sqlfilefull; # add WITH UR construction
then 
	sed -e 's/\(.*\)[Ww][Ii][Tt][Hh] [Uu][Rr]/\1 /'  -e '1!s/\(.*\);/\1 with ur;/' $v_sqlfilefull > $v_sqlfilefull"_tmp" 
	mv $v_sqlfilefull"_tmp" $v_sqlfilefull 
fi


echo_dt " - Connecting to the database."
# for debugging, normally the info returned by db2 is enough
# echo "CONNECT" $ETL_DB_READ_CONNECT
# echo "USER" $ETL_DB_READ_USER_ID
# echo "PW" $ETL_DB_READ_USER_PW
# echo "COMMAND" $cnncmd

db2 $cnncmd
if [ $? -gt 0 ]; then
  echo_dt " - ERROR - Cannot connect to $v_source_db."
  echo_fail
  exit 8
else
  db2 set schema $v_schema
  if [ $? -gt 0 ]; then
    echo_dt " - ERROR - Cannot set schema to $v_schema."
    echo_fail
    exit 8
  else
    echo_dt " - SUCCESS - Connected to $v_source_db, set schema $v_schema."
  fi
fi

RC=0

if [ "$v_extract_method" == "E" ]; then # extract via db2 export directly to file

  if [ -f $v_pre_sqlfilefull ]; then # does the pre-export sql file exist?
    echo_dt " - Running pre-SQL."
    v_starttimestamp_pre=`date '+%Y-%m-%d %H:%M:%S'`
    v_startseconds_pre=`date '+%s'`
    db2 -tvf  $v_pre_sqlfilefull > $v_tmpsqloutput.pre 2>&1 # then execute it in db2
    DB2RC=$?
    v_endseconds_pre=`date '+%s'`

    if [ $DB2RC -lt 4 ]; then
      let v_runtimeseconds_pre=$v_endseconds_pre-$v_startseconds_pre
      echo "Runtimestats (pre-SQL):"$v_starttimestamp_pre";"$v_runtimeseconds_pre
    else
      process_exception $v_tmpsqloutput.pre
      exit 8
    fi
  fi

  echo "export to \""$v_tmpexportfile2"\" of del modified by coldel; nochardel decplusblank striplzeros messages \""$v_tmpmsgfile"\"" > $v_tmpsqlfile
  echo_dt " - Running SQL, producing output to $v_tmpexportfile2"

  tail +2 "$v_sqlfilefull" >> "$v_tmpsqlfile"
  v_starttimestamp=`date '+%Y-%m-%d %H:%M:%S'`
  v_startseconds=`date '+%s'`
  db2 -t -f "$v_tmpsqlfile" > "$v_tmpsqloutput" 2>&1
  DB2RC=$?
  v_endseconds=`date '+%s'`
  db2 connect reset >/dev/null
  
  grep -q SQL3105N $v_tmpmsgfile > /dev/null 2>&1
  MAINRC=$?

  if [ $MAINRC -eq 0 -a $DB2RC -lt 4 ]; then
    
    v_rowsexported=`cat $v_tmpsqloutput | grep "Number of rows exported" | cut -d ":" -f2`
    let v_runtimeseconds=$v_endseconds-$v_startseconds
    echo "Runtimestats:"$v_starttimestamp";"$v_runtimeseconds";"$v_rowsexported

    echo_dt " - Preparing output file $v_tmpexportfile"
    head -1 "$v_sqlfilefull" | sed 's|^--||g' >"$v_tmpexportfile"
    cat "$v_tmpexportfile2" >>"$v_tmpexportfile"

    if [ "$v_zipfile" == "Y" ]; then
      gzip_exp
      mv_exp_path 1
    else
      mv_exp_path 3
    fi
  else
    process_exception $v_tmpmsgfile
  fi

else # extract via piping the select result into the target file

  echo_dt " - Running SQL, producing output to $v_tmpexportfile2"
  tail +2 "$v_sqlfilefull" > "$v_tmpsqlfile"
  v_starttimestamp=`date '+%Y-%m-%d %H:%M:%S'`
  v_startseconds=`date '+%s'`
  db2 -t -f "$v_tmpsqlfile" > $v_tmpexportfile2 2>&1
  DB2RC=$?
  v_endseconds=`date '+%s'`
  db2 connect reset >/dev/null
  grep -q "record(s) selected." $v_tmpexportfile2  > /dev/null 2>&1
  MAINRC=$?

  if [ $MAINRC -eq 0 -a $DB2RC -lt 4 ]; then
  
    v_rowsexported=`grep "record(s) selected" $v_tmpexportfile2 | tr -s " " | cut -d" " -f2`
    let v_runtimeseconds=$v_endseconds-$v_startseconds
    echo "Runtimestats:"$v_starttimestamp";"$v_runtimeseconds";"$v_rowsexported

    echo_dt " - Preparing output file $v_tmpexportfile"
    head -1 "$v_sqlfilefull" | sed 's|^--||g' >"$v_tmpexportfile"
    tail +3 "$v_tmpexportfile2" | grep ";" >>"$v_tmpexportfile"

    if [ "$v_zipfile" == "Y" ]; then
      gzip_exp
      mv_exp_path 1
    else
      mv_exp_path 3
    fi
  else
    tail -20 $v_tmpexportfile2 $v_tmpexportfile3
    process_exception $v_tmpexportfile3
  fi
fi

# cleanup
rm -f "$v_tmpsqlfile" "$v_tmpmsgfile" "$v_tmpsqloutput" "$v_tmpexportfile2" "$$v_tmpexportfile3"

exit $RC
