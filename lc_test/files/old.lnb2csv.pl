#!/usr/bin/perl -w
######################################################################
#
# process LNB data files, u01, u03, mpr and write data 
# out as csv files.
#
# NOTES:
#
# z_cass or feeder table no = FADD/10000
#
######################################################################
#
use strict;
#
use Carp;
use Getopt::Std;
use File::Find;
use File::Path qw(mkpath);
#
# use Memory::Usage;
#
######################################################################
#
# logical constants
#
use constant TRUE => 1;
use constant FALSE => 0;
#
# output types
#
use constant PROD_COMPLETE => 3;
use constant PROD_COMPLETE_LATER => 4;
use constant DETECT_CHANGE => 5;
use constant MANUAL_CLEAR => 11;
use constant TIMER_NOT_RUNNING => 12;
use constant AUTO_CLEAR => 13;
#
# processing states
#
use constant RESET => 'reset';
use constant BASELINE => 'baseline';
use constant DELTA => 'delta';
#
# common sections for all files types: u01, u03, mpr
#
use constant INDEX => '[Index]';
use constant INFORMATION => '[Information]';
#
# sections specific to u01
#
use constant TIME => '[Time]';
use constant CYCLETIME => '[CycleTime]';
use constant COUNT => '[Count]';
use constant DISPENSER => '[Dispenser]';
use constant MOUNTPICKUPFEEDER => '[MountPickupFeeder]';
use constant MOUNTPICKUPNOZZLE => '[MountPickupNozzle]';
use constant INSPECTIONDATA => '[InspectionData]';
#
# sections specific to u03
#
use constant BRECG => '[BRecg]';
use constant BRECGCALC => '[BRecgCalc]';
use constant ELAPSETIMERECOG => '[ElapseTimeRecog]';
use constant SBOARD => '[SBoard]';
use constant HEIGHTCORRECT => '[HeightCorrect]';
use constant MOUNTQUALITYTRACE => '[MountQualityTrace]';
use constant MOUNTLATESTREEL => '[MountLatestReel]';
use constant MOUNTEXCHANGEREEL => '[MountExchangeReel]';
#
# sections specfic to mpr
#
use constant TIMEDATASP => '[TimeDataSP]';
use constant COUNTDATASP => '[CountDataSP]';
use constant COUNTDATASP2 => '[CountDataSP2]';
use constant TRACEDATASP => '[TraceDataSP]';
use constant TRACEDATASP_2 => '[TraceDataSP_2]';
use constant ISPINFODATA => '[ISPInfoData]';
use constant MASKISPINFODATA => '[MaskISPInfoData]';
#
# files types
#
use constant LNB_U01_FILE_TYPE => 'u01';
use constant LNB_U03_FILE_TYPE => 'u03';
use constant LNB_MPR_FILE_TYPE => 'mpr';
#
# verbose levels
#
use constant NOVERBOSE => 0;
use constant MINVERBOSE => 1;
use constant MIDVERBOSE => 2;
use constant MAXVERBOSE => 3;
#
# processing options
#
use constant PROC_OPT_NONE => 0;
use constant PROC_OPT_IGNRESET12 => 1;
use constant PROC_OPT_IGNALL12 => 2;
use constant PROC_OPT_USENEGDELTS => 4;
use constant PROC_OPT_USEOLDNZ => 8;
#
# nozzle key names
#
use constant NZ_KEY_HEAD => 'Head';
use constant NZ_KEY_NHADD => 'NHAdd';
use constant NZ_KEY_NCADD => 'NCAdd';
#
use constant NZ_LABEL_NHADD_NCADD => 'nhadd_ncadd';
use constant NZ_LABEL_HEAD_NHADD => 'head_nhadd';
use constant NZ_LABEL_HEAD_NCADD => 'head_ncadd';
#
######################################################################
#
# globals
#
my $cmd = $0;
my $log_fh = *STDOUT;
# my $mu = Memory::Usage->new();
#
# cmd line options
#
my $logfile = '';
my $verbose = NOVERBOSE;
my $file_type = "all";
my $export_dir = '/tmp/';
my $proc_options = PROC_OPT_NONE;
my $remove_mount = FALSE;
#
my %verbose_levels =
(
    off => NOVERBOSE(),
    min => MINVERBOSE(),
    mid => MIDVERBOSE(),
    max => MAXVERBOSE()
);
#
my %allowed_proc_options =
(
    NONE => PROC_OPT_NONE(),
    IGNRESET12 => PROC_OPT_IGNRESET12(),
    IGNALL12 => PROC_OPT_IGNALL12(),
    USENEGDELTS => PROC_OPT_USENEGDELTS(),
    USEOLDNZ => PROC_OPT_USEOLDNZ()
);
#
# fields to ignore for output=12 files if enabled.
#
my %ignored_output12_fields =
(
    'TPICKUP' => 1,
    'TPMISS' => 1,
    'TRMISS' => 1,
    'TDMISS' => 1,
    'TMMISS' => 1,
    'THMISS' => 1,
    'CPERR' => 1,
    'CRERR' => 1,
    'CDERR' => 1,
    'CMERR' => 1,
    'CTERR' => 1
);
#
# summary tables.
#
my %totals = ();
#
# list of colums to export
#
my @mount_quality_trace_export_cols =
(
    { name => 'B', format => '%s' },
    { name => 'IDNUM', format => '%s' },
    { name => 'TURN', format => '%s' },
    { name => 'MS', format => '%s' },
    { name => 'TS', format => '%s' },
    { name => 'FAdd', format => '%s' },
    { name => 'FSAdd', format => '%s' },
    { name => 'FBLKCode', format => '%s' },
    { name => 'FBLKSerial', format => '%s' },
    { name => 'NHAdd', format => '%s' },
    { name => 'NCAdd', format => '%s' },
    { name => 'NBLKCode', format => '%s' },
    { name => 'NBLKSerial', format => '%s' },
    { name => 'ReelID', format => '%s' },
    { name => 'F', format => '%s' },
    { name => 'RCGX', format => '%s' },
    { name => 'RCGY', format => '%s' },
    { name => 'RCGA', format => '%s' },
    { name => 'TCX', format => '%s' },
    { name => 'TCY', format => '%s' },
    { name => 'MPosiRecX', format => '%s' },
    { name => 'MPosiRecY', format => '%s' },
    { name => 'MPosiRecA', format => '%s' },
    { name => 'MPosiRecZ', format => '%s' },
    { name => 'THMAX', format => '%s' },
    { name => 'THAVE', format => '%s' },
    { name => 'MNTCX', format => '%s' },
    { name => 'MNTCY', format => '%s' },
    { name => 'MNTCA', format => '%s' },
    { name => 'TLX', format => '%s' },
    { name => 'TLY', format => '%s' },
    { name => 'InspectArea', format => '%s' },
    { name => 'DIDNUM', format => '%s' },
    { name => 'DS', format => '%s' },
    { name => 'DispenseID', format => '%s' },
    { name => 'PARTS', format => '%s' },
    { name => 'WarpZ', format => '%s' }
);

my @feeder_export_cols =
(
    { name => 'Machine', format => '%s' },
    { name => 'Lane', format => ',%s' },
    { name => 'Stage', format => ',%s' },
    { name => 'FAdd', format => ',%s' },
    { name => 'FSAdd', format => ',%s' },
    { name => 'ReelID', format => ',%s' },
    { name => 'Pickup', format => ',%s' },
    { name => 'PMiss', format => ',%s' },
    { name => 'RMiss', format => ',%s' },
    { name => 'DMiss', format => ',%s' },
    { name => 'MMiss', format => ',%s' },
    { name => 'HMiss', format => ',%s' },
    { name => 'TRSMiss', format => ',%s' },
    { name => 'Mount', format => ',%s' }
);
#
my @feeder_export_cols2 =
(
    { name => 'Machine', format => '%s' },
    { name => 'Lane', format => ',%s' },
    { name => 'Stage', format => ',%s' },
    { name => 'FAdd', format => ',%s' },
    { name => 'FSAdd', format => ',%s' },
    { name => 'Pickup', format => ',%s' },
    { name => 'PMiss', format => ',%s' },
    { name => 'RMiss', format => ',%s' },
    { name => 'DMiss', format => ',%s' },
    { name => 'MMiss', format => ',%s' },
    { name => 'HMiss', format => ',%s' },
    { name => 'TRSMiss', format => ',%s' },
    { name => 'Mount', format => ',%s' }
);
#
my @feeder_export_cols3 =
(
    { name => 'Machine', format => '%s' },
    { name => 'Lane', format => ',%s' },
    { name => 'Stage', format => ',%s' },
    { name => 'TableNo', format => ',%s' },
    { name => 'Pickup', format => ',%s' },
    { name => 'PMiss', format => ',%s' },
    { name => 'RMiss', format => ',%s' },
    { name => 'DMiss', format => ',%s' },
    { name => 'MMiss', format => ',%s' },
    { name => 'HMiss', format => ',%s' },
    { name => 'TRSMiss', format => ',%s' },
    { name => 'Mount', format => ',%s' }
);
#
my @feeder_count_cols =
(
    'Pickup',
    'PMiss',
    'RMiss',
    'DMiss',
    'MMiss',
    'HMiss',
    'TRSMiss',
    'Mount'
);
#
my @nozzle_export_cols =
(
    { name => 'Machine', format => '%s' },
    { name => 'Lane', format => ',%s' },
    { name => 'Stage', format => ',%s' },
    { name => 'NHAdd', format => ',%s' },
    { name => 'NCAdd', format => ',%s' },
    { name => 'Blkserial', format => ',%s' },
    { name => 'Pickup', format => ',%s' },
    { name => 'PMiss', format => ',%s' },
    { name => 'RMiss', format => ',%s' },
    { name => 'DMiss', format => ',%s' },
    { name => 'MMiss', format => ',%s' },
    { name => 'HMiss', format => ',%s' },
    { name => 'TRSMiss', format => ',%s' },
    { name => 'Mount', format => ',%s' }
);
#
my @nozzle_export_cols2 =
(
    { name => 'Machine', format => '%s' },
    { name => 'Lane', format => ',%s' },
    { name => 'Stage', format => ',%s' },
    { name => 'NHAdd', format => ',%s' },
    { name => 'NCAdd', format => ',%s' },
    { name => 'Pickup', format => ',%s' },
    { name => 'PMiss', format => ',%s' },
    { name => 'RMiss', format => ',%s' },
    { name => 'DMiss', format => ',%s' },
    { name => 'MMiss', format => ',%s' },
    { name => 'HMiss', format => ',%s' },
    { name => 'TRSMiss', format => ',%s' },
    { name => 'Mount', format => ',%s' }
);
#
my %nozzle_export_cols_new =
(
    NZ_LABEL_NHADD_NCADD() => [
        { name => 'Machine', format => '%s' },
        { name => 'Lane', format => ',%s' },
        { name => 'Stage', format => ',%s' },
        { name => NZ_KEY_NHADD(), format => ',%s' },
        { name => NZ_KEY_NCADD(), format => ',%s' },
        { name => 'Blkserial', format => ',%s' },
        { name => 'Pickup', format => ',%s' },
        { name => 'PMiss', format => ',%s' },
        { name => 'RMiss', format => ',%s' },
        { name => 'DMiss', format => ',%s' },
        { name => 'MMiss', format => ',%s' },
        { name => 'HMiss', format => ',%s' },
        { name => 'TRSMiss', format => ',%s' },
        { name => 'Mount', format => ',%s' }
    ],
    NZ_LABEL_HEAD_NHADD() => [
        { name => 'Machine', format => '%s' },
        { name => 'Lane', format => ',%s' },
        { name => 'Stage', format => ',%s' },
        { name => NZ_KEY_HEAD(), format => ',%s' },
        { name => NZ_KEY_NHADD(), format => ',%s' },
        { name => 'Blkserial', format => ',%s' },
        { name => 'Pickup', format => ',%s' },
        { name => 'PMiss', format => ',%s' },
        { name => 'RMiss', format => ',%s' },
        { name => 'DMiss', format => ',%s' },
        { name => 'MMiss', format => ',%s' },
        { name => 'HMiss', format => ',%s' },
        { name => 'TRSMiss', format => ',%s' },
        { name => 'Mount', format => ',%s' }
    ],
    NZ_LABEL_HEAD_NCADD() => [
        { name => 'Machine', format => '%s' },
        { name => 'Lane', format => ',%s' },
        { name => 'Stage', format => ',%s' },
        { name => NZ_KEY_HEAD(), format => ',%s' },
        { name => NZ_KEY_NCADD(), format => ',%s' },
        { name => 'Blkserial', format => ',%s' },
        { name => 'Pickup', format => ',%s' },
        { name => 'PMiss', format => ',%s' },
        { name => 'RMiss', format => ',%s' },
        { name => 'DMiss', format => ',%s' },
        { name => 'MMiss', format => ',%s' },
        { name => 'HMiss', format => ',%s' },
        { name => 'TRSMiss', format => ',%s' },
        { name => 'Mount', format => ',%s' }
    ]
);
#
my %nozzle_export_cols2_new =
(
    NZ_LABEL_NHADD_NCADD() => [
        { name => 'Machine', format => '%s' },
        { name => 'Lane', format => ',%s' },
        { name => 'Stage', format => ',%s' },
        { name => NZ_KEY_NHADD(), format => ',%s' },
        { name => NZ_KEY_NCADD(), format => ',%s' },
        { name => 'Pickup', format => ',%s' },
        { name => 'PMiss', format => ',%s' },
        { name => 'RMiss', format => ',%s' },
        { name => 'DMiss', format => ',%s' },
        { name => 'MMiss', format => ',%s' },
        { name => 'HMiss', format => ',%s' },
        { name => 'TRSMiss', format => ',%s' },
        { name => 'Mount', format => ',%s' }
    ],
    NZ_LABEL_HEAD_NHADD() => [
        { name => 'Machine', format => '%s' },
        { name => 'Lane', format => ',%s' },
        { name => 'Stage', format => ',%s' },
        { name => NZ_KEY_HEAD(), format => ',%s' },
        { name => NZ_KEY_NHADD(), format => ',%s' },
        { name => 'Pickup', format => ',%s' },
        { name => 'PMiss', format => ',%s' },
        { name => 'RMiss', format => ',%s' },
        { name => 'DMiss', format => ',%s' },
        { name => 'MMiss', format => ',%s' },
        { name => 'HMiss', format => ',%s' },
        { name => 'TRSMiss', format => ',%s' },
        { name => 'Mount', format => ',%s' }
    ],
    NZ_LABEL_HEAD_NCADD() => [
        { name => 'Machine', format => '%s' },
        { name => 'Lane', format => ',%s' },
        { name => 'Stage', format => ',%s' },
        { name => NZ_KEY_HEAD(), format => ',%s' },
        { name => NZ_KEY_NCADD(), format => ',%s' },
        { name => 'Pickup', format => ',%s' },
        { name => 'PMiss', format => ',%s' },
        { name => 'RMiss', format => ',%s' },
        { name => 'DMiss', format => ',%s' },
        { name => 'MMiss', format => ',%s' },
        { name => 'HMiss', format => ',%s' },
        { name => 'TRSMiss', format => ',%s' },
        { name => 'Mount', format => ',%s' }
    ]
);
#
my @nozzle_count_cols =
(
    'Pickup',
    'PMiss',
    'RMiss',
    'DMiss',
    'MMiss',
    'HMiss',
    'TRSMiss',
    'Mount'
);
#
########################################################################
########################################################################
#
# miscellaneous functions
#
sub short_usage
{
    my ($arg0) = @_;
    print $log_fh <<EOF;

usage: $arg0 [-?] [-h] [-H] [-M] \\ 
        [-w | -W |-v level] \\ 
        [-t u10|u03|mpr] \\ 
        [-l logfile] \\ 
        [-o option] \\ 
        [-d path] \\
        directory ...

where:
    -? or -h - print this usage.
    -H - print long usage and description.
    -M - remove Mount fields (not in older files).
    -w - enable warning (level=min=1)
    -W - enable warning and trace (level=mid=2)
    -v - verbose level: 0=off,1=min,2=mid,3=max
    -t file-type = type of file to process: u01, u03, mpr.
                   default is all files.
    -l logfile - log file path
    -o option - enable a procesing option:
                ignreset12 - ignore resetable output=12 fields.
                ignall12 - ignore all output=12 files.
                usenegdelts - use negative deltas in calculations.
                useoldnz - use old nozzle processing.
    -d path - export directory, defaults to '/tmp'.

EOF
}
sub long_usage
{
    my ($arg0) = @_;
    print $log_fh <<EOF;

usage: $arg0 [-?] [-h] [-H] [-M] \\ 
        [-w | -W |-v level] \\ 
        [-t u10|u03|mpr] \\ 
        [-l logfile] \\ 
        [-o option] \\ 
        [-d path] \\
        directory ...

where:
    -? or -h - print this usage.
    -H - print long usage and description.
    -M - remove Mount fields (not in older files).
    -w - enable warning (level=min=1)
    -W - enable warning and trace (level=mid=2)
    -v - verbose level: 0=off,1=min,2=mid,3=max
    -t file-type = type of file to process: u01, u03, mpr.
                   default is all files.
    -l logfile - log file path
    -o option - enable a procesing option:
                ignreset12 - ignore resetable output=12 fields.
                ignall12 - ignore all output=12 files.
                usenegdelts - use negative deltas in calculations.
                useoldnz - use old nozzle processing.
    -d path - export directory, defaults to '/tmp'.

Description:

The script scans the list of given directories for U01, U03 and 
MPR files, then it processes the files.

For U01 files, the data in the following sections are tabulated
and reported in CSV files:

    [Time]
    [Count]
    [MountPickupFeeder]
    [MountPickupNozzle]

The CSV files are list below. The names indicate how the data
were grouped, that is, what keys were used:

    TIME_BY_MACHINE.csv
    TIME_BY_MACHINE_LANE.csv
    TIME_BY_PRODUCT_MACHINE.csv
    TIME_BY_PRODUCT_MACHINE_LANE.csv
    TIME_TOTALS_BY_PRODUCT.csv
    TIME_TOTALS.csv

    COUNT_BY_MACHINE.csv
    COUNT_BY_MACHINE_LANE.csv
    COUNT_BY_PRODUCT_MACHINE.csv
    COUNT_BY_PRODUCT_MACHINE_LANE.csv
    COUNT_TOTALS_BY_PRODUCT.csv
    COUNT_TOTALS.csv

    FEEDER_BY_MACHINE_LANE_STAGE_FADD_FSADD.csv
    FEEDER_BY_MACHINE_LANE_STAGE_FADD_FSADD_REELID.csv
    FEEDER_BY_MACHINE_LANE_STAGE_TABLE_NO.csv
    FEEDER_BY_PRODUCT_MACHINE_LANE_STAGE_FADD_FSADD.csv
    FEEDER_BY_PRODUCT_MACHINE_LANE_STAGE_FADD_FSADD_REELID.csv
    FEEDER_BY_PRODUCT_MACHINE_LANE_STAGE_TABLE_NO.csv

    NOZZLE_BY_MACHINE_LANE_STAGE_HEAD_NCADD_BLKSERIAL.csv
    NOZZLE_BY_MACHINE_LANE_STAGE_HEAD_NCADD.csv
    NOZZLE_BY_MACHINE_LANE_STAGE_HEAD_NHADD_BLKSERIAL.csv
    NOZZLE_BY_MACHINE_LANE_STAGE_HEAD_NHADD.csv
    NOZZLE_BY_MACHINE_LANE_STAGE_NHADD_NCADD_BLKSERIAL.csv
    NOZZLE_BY_MACHINE_LANE_STAGE_NHADD_NCADD.csv
    NOZZLE_BY_PRODUCT_MACHINE_LANE_STAGE_HEAD_NCADD_BLKSERIAL.csv
    NOZZLE_BY_PRODUCT_MACHINE_LANE_STAGE_HEAD_NCADD.csv
    NOZZLE_BY_PRODUCT_MACHINE_LANE_STAGE_HEAD_NHADD_BLKSERIAL.csv
    NOZZLE_BY_PRODUCT_MACHINE_LANE_STAGE_HEAD_NHADD.csv
    NOZZLE_BY_PRODUCT_MACHINE_LANE_STAGE_NHADD_NCADD_BLKSERIAL.csv
    NOZZLE_BY_PRODUCT_MACHINE_LANE_STAGE_NHADD_NCADD.csv

The U01 file raw data are written to separate files by section. The 
following list of files is generated:

    TIME_BY_MACHINE_LANE_STAGE_FILENAME.csv
    TIME_BY_PRODUCT_MACHINE_LANE_STAGE_FILENAME.csv
    CYCLE_TIME_BY_MACHINE_LANE_STAGE_FILENAME.csv
    CYCLE_TIME_BY_PRODUCT_MACHINE_LANE_STAGE_FILENAME.csv
    COUNT_BY_MACHINE_LANE_STAGE_FILENAME.csv
    COUNT_BY_PRODUCT_MACHINE_LANE_STAGE_FILENAME.csv
    DISPENSER_BY_MACHINE_LANE_STAGE_FILENAME.csv
    DISPENSER_BY_PRODUCT_MACHINE_LANE_STAGE_FILENAME.csv
    MOUNT_PICKUP_FEEDER_BY_MACHINE_LANE_STAGE_FILENAME.csv
    MOUNT_PICKUP_FEEDER_BY_PRODUCT_MACHINE_LANE_STAGE_FILENAME.csv
    MOUNT_PICKUP_NOZZLE_BY_MACHINE_LANE_STAGE_FILENAME.csv
    MOUNT_PICKUP_NOZZLE_BY_PRODUCT_MACHINE_LANE_STAGE_FILENAME.csv
    INSPECTION_DATA_BY_MACHINE_LANE_STAGE_FILENAME.csv
    INSPECTION_DATA_BY_PRODUCT_MACHINE_LANE_STAGE_FILENAME.csv

The U03 file raw data are written to separate files by section. The 
following list of files is generated:

    MOUNT_QUALITY_TRACE_BY_MACHINE_LANE_STAGE_FILENAME.csv
    MOUNT_QUALITY_TRACE_BY_PRODUCT_MACHINE_LANE_STAGE_FILENAME.csv
    MOUNT_LATEST_REEL_BY_MACHINE_LANE_STAGE_FILENAME.csv
    MOUNT_LATEST_REEL_BY_PRODUCT_MACHINE_LANE_STAGE_FILENAME.csv
    MOUNT_EXCHANGE_REEL_BY_MACHINE_LANE_STAGE_FILENAME.csv
    MOUNT_EXCHANGE_REEL_BY_PRODUCT_MACHINE_LANE_STAGE_FILENAME.csv

The MPR file raw data are written to separate files by section. The 
following list of files is generated:

    TIME_DATA_SP_BY_MACHINE_LANE_STAGE_FILENAME.csv
    TIME_DATA_SP_BY_PRODUCT_MACHINE_LANE_STAGE_FILENAME.csv
    COUNT_DATA_SP_BY_MACHINE_LANE_STAGE_FILENAME.csv
    COUNT_DATA_SP_BY_PRODUCT_MACHINE_LANE_STAGE_FILENAME.csv
    COUNT_DATA_SP2_BY_MACHINE_LANE_STAGE_FILENAME.csv
    COUNT_DATA_SP2_BY_PRODUCT_MACHINE_LANE_STAGE_FILENAME.csv
    TRACE_DATA_SP_BY_MACHINE_LANE_STAGE_FILENAME.csv
    TRACE_DATA_SP_BY_PRODUCT_MACHINE_LANE_STAGE_FILENAME.csv
    TRACE_DATA_SP_2_BY_MACHINE_LANE_STAGE_FILENAME.csv
    TRACE_DATA_SP_2_BY_PRODUCT_MACHINE_LANE_STAGE_FILENAME.csv
    ISP_INFO_DATA_BY_MACHINE_LANE_STAGE_FILENAME.csv
    ISP_INFO_DATA_BY_PRODUCT_MACHINE_LANE_STAGE_FILENAME.csv
    MASK_ISP_INFO_DATA_BY_MACHINE_LANE_STAGE_FILENAME.csv
    MASK_ISP_INFO_DATA_BY_PRODUCT_MACHINE_LANE_STAGE_FILENAME.csv

The command line options '-?' and '-h' list a short version of the
usage message. This is the default usage message. Option '-H'
prints out a more detailed version of the usage. This one.

The option '-M' is a hack. Since older U01 and MPR files may not
support the 'Mount' column, this option removes any reference
to the 'Mount' field during processing. If you do not use it on
older files, you will roll in "undefined field" errors.

There are four verbose levels:

1) The default value is 0 which prints out no warnings. Only error 
messages are printed when the script exits because of a fatal error. 

2) Level 1 which is set eith with '-w' or '-v 1' prints out non-fatal 
warnings. This includes warning for negative deltas, changes in 
blkserial or reel id, change overs, etc. The warnings highlight 
events which may be of interest. I usually run with this warning 
level when debugging.

3) Level 2 which is set with '-W' or '-v 2' includes all the data
include with level 1 and 0, and additional messages for tracing. It
can generate a lot of messages.

4) Level 3 which is set with '-v 3' generates the most messages. It
will list the data which are read in, etc. It is *very* verbose.

If you wish to limit the file processing to only one type of 
file, then use the the '-t' option and choose the type: u01, u03,
or mpr. The default is all types of files if the file type is found.

You can set the output file name using the '-l' option. You give
it the name of the file. By default all output goes to STDOUT. 

The CSV files are written by default in /tmp. If you wish to 
use a different directory, then use the '-d' option and give
the path as the option argument.

The '-o' option allows you to change how the U01 tabulation is
performed. The following options are available:

ignreset12 - ignore resetable output=12 fields. This options causes
the data in the [Count] section of a U01, output=12 file to be
completely ignored.

ignall12 - ignore all output=12 files. Ths option cause all 
U01, output=12 files to be ignored in all tabulations.

usenegdelts - use negative deltas in calculations. This option
causes all negative deltas to be used in tabulations. The default
is to set any negative delta to zero.

useoldnz - use old nozzle processing. This is strictly for testing.
Do not use.

EOF
}
#
sub remove_mount_fields
{
    #
    # it's a hack. since some older U01 and MPR files do not
    # have the Mount column, we have to remove any reference
    # to it in any internal data. 
    #
    @feeder_export_cols = 
        grep { $_->{name} ne 'Mount' } @feeder_export_cols;
    @feeder_export_cols2 = 
        grep { $_->{name} ne 'Mount' } @feeder_export_cols2;
    @feeder_export_cols3 = 
        grep { $_->{name} ne 'Mount' } @feeder_export_cols3;
    @feeder_count_cols = 
        grep { $_ ne 'Mount' } @feeder_count_cols;
    #
    @nozzle_export_cols = 
        grep { $_->{name} ne 'Mount' } @nozzle_export_cols;
    @nozzle_export_cols2 = 
        grep { $_->{name} ne 'Mount' } @nozzle_export_cols2;
    @nozzle_count_cols = 
        grep { $_ ne 'Mount' } @nozzle_count_cols;
    #
    foreach my $key (keys %nozzle_export_cols_new)
    {
        @{$nozzle_export_cols_new{$key}} =
            grep { $_->{name} ne 'Mount' } 
                @{$nozzle_export_cols_new{$key}};
    }
    foreach my $key (keys %nozzle_export_cols2_new)
    {
        @{$nozzle_export_cols2_new{$key}} =
            grep { $_->{name} ne 'Mount' } 
                @{$nozzle_export_cols2_new{$key}};
    }
}
#
sub set_name_value_section_column_names
{
    my ($file_type, $pfile, $section) = @_;
    #
    if ( ! exists($pfile->{$section}))
    {
        printf $log_fh "%d: No column data for %s %s.\n", __LINE__, $file_type, $section if ($verbose >= MAXVERBOSE);
    }
    elsif ( ! exists($totals{column_names}{$file_type}{$section}) )
    {
        @{$totals{column_names}{$file_type}{$section}} = 
            (sort keys %{$pfile->{$section}->{data}});
        #
        printf $log_fh "\n%d: Setting column names %s %s: %s\n", __LINE__, $file_type, $section, join(' ', @{$totals{column_names}{$file_type}{$section}});
    }
}
#
sub set_list_section_column_names
{
    my ($file_type, $pfile, $section) = @_;
    #
    if ( ! exists($pfile->{$section}))
    {
        printf $log_fh "%d: No column data for %s %s.\n", __LINE__, $file_type, $section if ($verbose >= MAXVERBOSE);
    }
    elsif ( ! exists($totals{column_names}{$file_type}{$section}) )
    {
        my $pcols = $pfile->{$section}->{column_names};
        $totals{column_names}{$file_type}{$section} = $pcols;
        #
        printf $log_fh "\n%d: Setting column names %s %s: %s\n", __LINE__, $file_type, $section, join(' ', @{$totals{column_names}{$file_type}{$section}});
    }
}
#
sub export_list_section_as_csv
{
    my ($section, $file_type, $file_name, $machine_label, $do_product) = @_;
    #
    if ( ! exists($totals{$section}))
    {
        printf $log_fh "\n%d: Section %s does NOT exist\n", __LINE__, $section;
        return;
    }
    #
    ###############################################################
    #
    printf $log_fh "\n%d: Export Data For %s by Machine, Lane, Stage, Filename:\n", __LINE__, $section;
    #
    my $outfnm = "${export_dir}/${file_name}_BY_MACHINE_LANE_STAGE_FILENAME.csv";
    printf $log_fh "%d: File %s already exists\n", __LINE__, $outfnm if ( -e $outfnm);
    open(my $outfh, ">" , $outfnm) || die $!;
    #
    my $pcols = $totals{column_names}{$file_type}{$section};
    #
    printf $outfh "${machine_label},lane,stage,filename";
    foreach my $col (@{$pcols})
    {
        printf $outfh ",%s", $col;
    }
    printf $outfh "\n";
    #
    foreach my $machine (sort { $a <=> $b } keys %{$totals{$section}{by_machine_lane_stage_filename}})
    {
        foreach my $lane (sort { $a <=> $b } keys %{$totals{$section}{by_machine_lane_stage_filename}{$machine}})
        {
            foreach my $stage (sort { $a <=> $b } keys %{$totals{$section}{by_machine_lane_stage_filename}{$machine}{$lane}})
            {
                foreach my $filename (sort keys %{$totals{$section}{by_machine_lane_stage_filename}{$machine}{$lane}{$stage}})
                {
                    foreach my $prow (@{$totals{$section}{by_machine_lane_stage_filename}{$machine}{$lane}{$stage}{$filename}{data}})
                    {
                        printf $outfh "%s,%s,%s,%s", $machine, $lane, $stage, $filename;
                        foreach my $col (@{$pcols})
                        {
                            printf $outfh ",%s", $prow->{$col};
                        }
                        printf $outfh "\n";
                    }
                }
            }
        }
    }
    close($outfh);
    #
    return unless ($do_product == TRUE);
    #
    ###############################################################
    #
    printf $log_fh "\n%d: Export Data For %s by Product, Machine, Lane, Stage, Filename:\n", __LINE__, $section;
    #
    $outfnm = "${export_dir}/${file_name}_BY_PRODUCT_MACHINE_LANE_STAGE_FILENAME.csv";
    printf $log_fh "%d: File %s already exists\n", __LINE__, $outfnm if ( -e $outfnm);
    open($outfh, ">" , $outfnm) || die $!;
    #
    printf $outfh "product,${machine_label},lane,stage,filename";
    foreach my $col (@{$pcols})
    {
        printf $outfh ",%s", $col;
    }
    printf $outfh "\n";
    #
    foreach my $product (sort keys %{$totals{$section}{by_product}})
    {
        foreach my $machine (sort { $a <=> $b } keys %{$totals{$section}{by_product}{$product}{by_machine_lane_stage_filename}})
        {
            foreach my $lane (sort { $a <=> $b } keys %{$totals{$section}{by_product}{$product}{by_machine_lane_stage_filename}{$machine}})
            {
                foreach my $stage (sort { $a <=> $b } keys %{$totals{$section}{by_product}{$product}{by_machine_lane_stage_filename}{$machine}{$lane}})
                {
                    foreach my $filename (sort keys %{$totals{$section}{by_product}{$product}{by_machine_lane_stage_filename}{$machine}{$lane}{$stage}})
                    {
                        foreach my $prow (@{$totals{$section}{by_product}{$product}{by_machine_lane_stage_filename}{$machine}{$lane}{$stage}{$filename}{data}})
                        {
                            printf $outfh "%s,%s,%s,%s,%s", $product, $machine, $lane, $stage, $filename;
                            foreach my $col (@{$pcols})
                            {
                                printf $outfh ",%s", $prow->{$col};
                            }
                            printf $outfh "\n";
                        }
                    }
                }
            }
        }
    }
    close($outfh);
}
#
sub export_name_value_section_as_csv
{
    my ($section, $file_type, $file_name, $machine_label, $do_product) = @_;
    #
    if ( ! exists($totals{$section}))
    {
        printf $log_fh "\n%d: Section %s does NOT exist\n", __LINE__, $section;
        return;
    }
    #
    ###############################################################
    #
    printf $log_fh "\n%d: Export Data For %s by Machine, Lane, Stage, Filename:\n", __LINE__, $section;
    #
    my $outfnm = "${export_dir}/${file_name}_BY_MACHINE_LANE_STAGE_FILENAME.csv";
    printf $log_fh "%d: File %s already exists\n", __LINE__, $outfnm if ( -e $outfnm);
    open(my $outfh, ">" , $outfnm) || die $!;
    #
    my $pcols = $totals{column_names}{$file_type}{$section};
    #
    printf $outfh "${machine_label},lane,stage,filename";
    foreach my $col (@{$pcols})
    {
        printf $outfh ",%s", $col;
    }
    printf $outfh "\n";
    #
    foreach my $machine (sort { $a <=> $b } keys %{$totals{$section}{by_machine_lane_stage_filename}})
    {
        foreach my $lane (sort { $a <=> $b } keys %{$totals{$section}{by_machine_lane_stage_filename}{$machine}})
        {
            foreach my $stage (sort { $a <=> $b } keys %{$totals{$section}{by_machine_lane_stage_filename}{$machine}{$lane}})
            {
                foreach my $filename (sort keys %{$totals{$section}{by_machine_lane_stage_filename}{$machine}{$lane}{$stage}})
                {
                    printf $outfh "%s,%s,%s,%s", $machine, $lane, $stage, $filename;
                    foreach my $col (@{$pcols})
                    {
                        printf $outfh ",%s", $totals{$section}{by_machine_lane_stage_filename}{$machine}{$lane}{$stage}{$filename}{data}{$col};
                    }
                    printf $outfh "\n";
                }
            }
        }
    }
    close($outfh);
    #
    return unless ($do_product == TRUE);
    #
    ###############################################################
    #
    printf $log_fh "\n%d: Export Data For %s by Product, Machine, Lane, Stage, Filename:\n", __LINE__, $section;
    #
    $outfnm = "${export_dir}/${file_name}_BY_PRODUCT_MACHINE_LANE_STAGE_FILENAME.csv";
    printf $log_fh "%d: File %s already exists\n", __LINE__, $outfnm if ( -e $outfnm);
    open($outfh, ">" , $outfnm) || die $!;
    #
    printf $outfh "product,${machine_label},lane,stage,filename";
    foreach my $col (@{$pcols})
    {
        printf $outfh ",%s", $col;
    }
    printf $outfh "\n";
    #
    foreach my $product (sort keys %{$totals{$section}{by_product}})
    {
        foreach my $machine (sort { $a <=> $b } keys %{$totals{$section}{by_product}{$product}{by_machine_lane_stage_filename}})
        {
            foreach my $lane (sort { $a <=> $b } keys %{$totals{$section}{by_product}{$product}{by_machine_lane_stage_filename}{$machine}})
            {
                foreach my $stage (sort { $a <=> $b } keys %{$totals{$section}{by_product}{$product}{by_machine_lane_stage_filename}{$machine}{$lane}})
                {
                    foreach my $filename (sort keys %{$totals{$section}{by_product}{$product}{by_machine_lane_stage_filename}{$machine}{$lane}{$stage}})
                    {
                        printf $outfh "%s,%s,%s,%s,%s", $product, $machine, $lane, $stage, $filename;
                        foreach my $col (@{$pcols})
                        {
                            printf $outfh ",%s", $totals{$section}{by_machine_lane_stage_filename}{$machine}{$lane}{$stage}{$filename}{data}{$col};
                        }
                        printf $outfh "\n";
                    }
                }
            }
        }
    }
    close($outfh);
}
#
sub tabulate_list_section
{
    my ($pdb, $pfile, $file_type, $section, $do_product) = @_;
    #
    my $filename = $pfile->{file_name};
    my $machine = $pfile->{mach_no};
    my $lane = $pfile->{lane};
    my $stage = $pfile->{stage};
    my $output_no = $pfile->{output_no};
    #
    #
    if ( ! exists($pfile->{$section}))
    {
        printf $log_fh "%d: WARNING: Section %s does NOT exist in file %s\n", __LINE__, $section, $filename if ($verbose >= MINVERBOSE);
        return;
    }
    #
    @{$totals{$section}{by_machine_lane_stage_filename}{$machine}{$lane}{$stage}{$filename}{data}} = ();
    #
    foreach my $prow (@{$pfile->{$section}->{data}})
    {
        unshift @{$totals{$section}{by_machine_lane_stage_filename}{$machine}{$lane}{$stage}{$filename}{data}}, $prow;
    }
    #
    return unless ($do_product == TRUE);
    #
    my $product = $pdb->{product}{$file_type}{$machine}{$lane}{$stage};
    #
    @{$totals{$section}{by_product}{$product}{by_machine_lane_stage_filename}{$machine}{$lane}{$stage}{$filename}{data}} = ();
    #
    foreach my $prow (@{$pfile->{$section}->{data}})
    {
        unshift @{$totals{$section}{by_product}{$product}{by_machine_lane_stage_filename}{$machine}{$lane}{$stage}{$filename}{data}}, $prow;
    }
}
#
sub prepare_list_section
{
    my ($pdb, $pfile, $file_type, $section, $do_product) = @_;
    #
    if ($pfile->{found_data}->{$section} == FALSE)
    {
        printf $log_fh "%d: WARNING: No data for %s.\n", __LINE__, $section if ($verbose >= MIDVERBOSE);
        return;
    }
    #
    my $machine = $pfile->{mach_no};
    my $lane = $pfile->{lane};
    my $stage = $pfile->{stage};
    my $output_no = $pfile->{output_no};
    my $filename = $pfile->{file_name};
    #
    set_list_section_column_names($file_type, $pfile, $section);
    #
    printf $log_fh "\n%d: SECTION  : %s\n", __LINE__, $section 
        if ($verbose >= MAXVERBOSE);
    #
    if ($verbose >= MAXVERBOSE)
    {
        printf $log_fh "%d: MACHINE  : %s\n", __LINE__, $machine;
        printf $log_fh "%d: LANE     : %d\n", __LINE__, $lane;
        printf $log_fh "%d: STAGE    : %d\n", __LINE__, $stage;
        printf $log_fh "%d: OUTPUT NO: %s\n", __LINE__, $output_no;
        printf $log_fh "%d: FILE RECS : %d\n", __LINE__, scalar(@{$pfile->{data}});
        printf $log_fh "%d: %s RECS: %d\n", __LINE__, $section, scalar(@{$pfile->{$section}->{data}}) if (defined(@{$pfile->{$section}->{data}}));
    }
    #
    tabulate_list_section($pdb, $pfile, $file_type, $section, $do_product);
    #
    return;
}
#
sub tabulate_name_value_section
{
    my ($pdb, $pfile, $file_type, $section, $do_product) = @_;
    #
    my $filename = $pfile->{file_name};
    my $machine = $pfile->{mach_no};
    my $lane = $pfile->{lane};
    my $stage = $pfile->{stage};
    my $output_no = $pfile->{output_no};
    #
    if ( ! exists($pfile->{$section}))
    {
        printf $log_fh "%d: WARNING: Section %s does NOT exist in file %s\n", __LINE__, $section, $filename if ($verbose >= MINVERBOSE);
        return;
    }
    #
    foreach my $key (keys %{$pfile->{$section}->{data}})
    {
        $totals{$section}{by_machine_lane_stage_filename}{$machine}{$lane}{$stage}{$filename}{data}{$key} = $pfile->{$section}->{data}->{$key};
    }
    #
    return unless ($do_product == TRUE);
    #
    my $product = $pdb->{product}{$file_type}{$machine}{$lane}{$stage};
    #
    foreach my $key (keys %{$pfile->{$section}->{data}})
    {
        $totals{$section}{by_product}{$product}{by_machine_lane_stage_filename}{$machine}{$lane}{$stage}{$filename}{data}{$key} = $pfile->{$section}->{data}->{$key};
    }
}
#
sub prepare_name_value_section
{
    my ($pdb, $pfile, $file_type, $section, $do_product) = @_;
    #
    if ($pfile->{found_data}->{$section} == FALSE)
    {
        printf $log_fh "%d: WARNING: No data for %s.\n", __LINE__, $section if ($verbose >= MIDVERBOSE);
        return;
    }
    #
    my $machine = $pfile->{mach_no};
    my $lane = $pfile->{lane};
    my $stage = $pfile->{stage};
    my $output_no = $pfile->{output_no};
    my $filename = $pfile->{file_name};
    #
    set_name_value_section_column_names($file_type, $pfile, $section);
    #
    printf $log_fh "\n%d: SECTION  : %s\n", __LINE__, $section 
        if ($verbose >= MAXVERBOSE);
    #
    if ($verbose >= MAXVERBOSE)
    {
        printf $log_fh "%d: MACHINE  : %s\n", __LINE__, $machine;
        printf $log_fh "%d: LANE     : %d\n", __LINE__, $lane;
        printf $log_fh "%d: STAGE    : %d\n", __LINE__, $stage;
        printf $log_fh "%d: OUTPUT NO: %s\n", __LINE__, $output_no;
        printf $log_fh "%d: FILE RECS : %d\n", __LINE__, scalar(@{$pfile->{data}});
        printf $log_fh "%d: %s RECS: %d\n", __LINE__, $section, scalar(keys %{$pfile->{$section}->{data}}) if (defined(keys %{$pfile->{$section}->{data}}));
    }
    #
    tabulate_name_value_section($pdb, $pfile, $file_type, $section, $do_product);
    #
    return;
}
#
########################################################################
########################################################################
#
# current product functions
#
sub get_product_info
{
    my ($pdata, $pmjsid, $plotname, $plotnumber) = @_;
    #
    my $section = INDEX;
    $$pmjsid = $pdata->{$section}->{data}->{MJSID};
    $$pmjsid = $1 if ($$pmjsid =~ m/"([^"]*)"/);
    #
    $section = INFORMATION;
    $$plotname = $pdata->{$section}->{data}->{LotName};
    $$plotname = $1 if ($$plotname =~ m/"([^"]*)"/);
    $$plotnumber = $pdata->{$section}->{data}->{LotNumber};
}
#
sub set_product_info
{
    my ($pdb, $pfile, $ftype) = @_;
    #
    my $filename = $pfile->{file_name};
    #
    my $machine = $pfile->{mach_no};
    my $lane = $pfile->{lane};
    my $stage = $pfile->{stage};
    my $output_no = $pfile->{output_no};
    #
    my $mjsid = 'UNKNOWN';
    my $lotname = 'UNKNOWN';
    my $lotnumber = 0;
    #
    if ( ! exists($pdb->{product}{$ftype}{$machine}{$lane}{$stage}))
    {
        $pdb->{product}{$ftype}{$machine}{$lane}{$stage} = "${mjsid}_${lotname}_${lotnumber}";
        $pdb->{change_over}{$ftype}{$machine}{$lane}{$stage} = FALSE;
    }
    elsif (($output_no == PROD_COMPLETE) ||
           ($output_no == PROD_COMPLETE_LATER))
    {
        get_product_info($pfile, \$mjsid, \$lotname, \$lotnumber);
        #
        if (($pdb->{product}{$ftype}{$machine}{$lane}{$stage} ne "${mjsid}_${lotname}_${lotnumber}") &&
            ($pdb->{product}{$ftype}{$machine}{$lane}{$stage} ne "UNKNOWN_UNKNOWN_0"))
        {
            $pdb->{change_over}{$ftype}{$machine}{$lane}{$stage} = TRUE;
        }
        else
        {
            $pdb->{change_over}{$ftype}{$machine}{$lane}{$stage} = FALSE;
        }
        #
        $pdb->{product}{$ftype}{$machine}{$lane}{$stage} = "${mjsid}_${lotname}_${lotnumber}";
    }
    else
    {
        # clear this flag.
        $pdb->{change_over}{$ftype}{$machine}{$lane}{$stage} = FALSE;
    }
    #
    printf $log_fh "%d: Product %s: %s, Change Over: %d\n", __LINE__, $ftype, $pdb->{product}{$ftype}{$machine}{$lane}{$stage}, $pdb->{change_over}{$ftype}{$machine}{$lane}{$stage} if ($verbose >= MIDVERBOSE);
}
#
########################################################################
########################################################################
#
# scan directories for U01, U03 and MPR files.
#
my %all_list = ();
my $one_type = '';
#
sub want_one_type
{
    if ($_ =~ m/^.*\.${one_type}$/)
    {
        printf $log_fh "%d: FOUND %s FILE: %s\n", __LINE__, $one_type, $File::Find::name 
            if ($verbose >= MAXVERBOSE);
        #
        my $file_name = $_;
        #
        my $date = '';
        my $mach_no = '';
        my $stage = '';
        my $lane = '';
        my $pcb_serial = '';
        my $pcb_id = '';
        my $output_no = '';
        my $pcb_id_lot_no = '';
        #
        my @parts = split('\+-\+', $file_name);
        if (scalar(@parts) >= 9)
        {
            $date          = $parts[0];
            $mach_no       = $parts[1];
            $stage         = $parts[2];
            $lane          = $parts[3];
            $pcb_serial    = $parts[4];
            $pcb_id        = $parts[5];
            $output_no     = $parts[6];
            $pcb_id_lot_no = $parts[7];
        }
        else
        {
            @parts = split('-', $file_name);
            if (scalar(@parts) >= 9)
            {
                $date          = $parts[0];
                $mach_no       = $parts[1];
                $stage         = $parts[2];
                $lane          = $parts[3];
                $pcb_serial    = $parts[4];
                $pcb_id        = $parts[5];
                $output_no     = $parts[6];
                $pcb_id_lot_no = $parts[7];
            }
        }
        #
        unshift @{$all_list{$one_type}},
        {
            'file_name'     => $file_name,
            'full_path'     => $File::Find::name,
            'directory'     => $File::Find::dir,
            'date'          => $date,
            'mach_no'       => $mach_no,
            'stage'         => $stage,
            'lane'          => $lane,
            'pcb_serial'    => $pcb_serial,
            'pcb_id'        => $pcb_id,
            'output_no'     => $output_no,
            'pcb_id_lot_no' => $pcb_id_lot_no
        };
    }
}
#
sub want_all_types
{
    my $dt = '';
    #
    if ($_ =~ m/^.*\.u01$/)
    {
        printf $log_fh "%d: FOUND u01 FILE: %s\n", __LINE__, $File::Find::name 
            if ($verbose >= MAXVERBOSE);
        $dt = 'u01';
    }
    elsif ($_ =~ m/^.*\.u03$/)
    {
        printf $log_fh "%d: FOUND u03 FILE: %s\n", __LINE__, $File::Find::name 
            if ($verbose >= MAXVERBOSE);
        $dt = 'u03';
    }
    elsif ($_ =~ m/^.*\.mpr$/)
    {
        printf $log_fh "%d: FOUND mpr FILE: %s\n", __LINE__, $File::Find::name 
            if ($verbose >= MAXVERBOSE);
        $dt = 'mpr';
    }
    #
    if ($dt ne '')
    {
        my $file_name = $_;
        #
        my $date = '';
        my $mach_no = '';
        my $stage = '';
        my $lane = '';
        my $pcb_serial = '';
        my $pcb_id = '';
        my $output_no = '';
        my $pcb_id_lot_no = '';
        #
        my @parts = split('\+-\+', $file_name);
        if (scalar(@parts) >= 9)
        {
            $date          = $parts[0];
            $mach_no       = $parts[1];
            $stage         = $parts[2];
            $lane          = $parts[3];
            $pcb_serial    = $parts[4];
            $pcb_id        = $parts[5];
            $output_no     = $parts[6];
            $pcb_id_lot_no = $parts[7];
        }
        else
        {
            @parts = split('-', $file_name);
            if (scalar(@parts) >= 9)
            {
                $date          = $parts[0];
                $mach_no       = $parts[1];
                $stage         = $parts[2];
                $lane          = $parts[3];
                $pcb_serial    = $parts[4];
                $pcb_id        = $parts[5];
                $output_no     = $parts[6];
                $pcb_id_lot_no = $parts[7];
            }
        }
        #
        unshift @{$all_list{$dt}},
        {
            'file_name'     => $file_name,
            'full_path'     => $File::Find::name,
            'directory'     => $File::Find::dir,
            'date'          => $date,
            'mach_no'       => $mach_no,
            'stage'         => $stage,
            'lane'          => $lane,
            'pcb_serial'    => $pcb_serial,
            'pcb_id'        => $pcb_id,
            'output_no'     => $output_no,
            'pcb_id_lot_no' => $pcb_id_lot_no
        };
    }
}
#
sub get_all_files
{
    my ($ftype, $pargv, $pu01, $pu03, $pmpr) = @_;
    #
    # optimize for file type
    #
    if ($ftype eq 'u01')
    {
        $one_type = $ftype;
        $all_list{$one_type} = $pu01;
        #
        find(\&want_one_type, @{$pargv});
        #
        @{$pu01} = sort { $a->{file_name} cmp $b->{file_name} } @{$pu01};
    }
    elsif ($ftype eq 'u03')
    {
        $one_type = $ftype;
        $all_list{$one_type} = $pu03;
        #
        find(\&want_one_type, @{$pargv});
        #
        @{$pu03} = sort { $a->{file_name} cmp $b->{file_name} } @{$pu03};
    }
    elsif ($ftype eq 'mpr')
    {
        $one_type = $ftype;
        $all_list{$one_type} = $pmpr;
        #
        find(\&want_one_type, @{$pargv});
        #
        @{$pmpr} = sort { $a->{file_name} cmp $b->{file_name} } @{$pmpr};
    }
    else
    {
        $all_list{u01} = $pu01;
        $all_list{u03} = $pu03;
        $all_list{mpr} = $pmpr;
        #
        find(\&want_all_types, @{$pargv});
        #
        @{$pu01} = sort { $a->{file_name} cmp $b->{file_name} } @{$pu01};
        @{$pu03} = sort { $a->{file_name} cmp $b->{file_name} } @{$pu03};
        @{$pmpr} = sort { $a->{file_name} cmp $b->{file_name} } @{$pmpr};
    }
}
#
######################################################################
######################################################################
#
# read in data file and load all sections
#
sub load
{
    my ($pdata) = @_;
    #
    my $path = $pdata->{full_path};
    #
    if ( ! -r $path )
    {
        printf $log_fh "\n%d: ERROR: file $path is NOT readable\n\n", __LINE__;
        return 0;
    }
    #
    unless (open(INFD, $path))
    {
        printf $log_fh "\n%d: ERROR: unable to open $path.\n\n", __LINE__;
        return 0;
    }
    @{$pdata->{data}} = <INFD>;
    close(INFD);
    #
    # remove newlines
    #
    chomp(@{$pdata->{data}});
    printf $log_fh "%d: Lines read: %d\n", __LINE__, scalar(@{$pdata->{data}})
        if ($verbose >= MAXVERBOSE);
    #
    return 1;
}
#
sub load_name_value
{
    my ($pdata, $section) = @_;
    #
    $pdata->{found_data}->{$section} = FALSE;
    #
    printf $log_fh "\n%d: Loading Name-Value Section: %s\n", __LINE__, $section
        if ($verbose >= MAXVERBOSE);
    #
    my $re_section = '\\' . $section;
    @{$pdata->{raw}->{$section}} = 
        grep /^${re_section}\s*$/ .. /^\s*$/, @{$pdata->{data}};
    #
    # printf $log_fh "<%s>\n", join("\n", @{$pdata->{raw}->{$section}});
    #
    if (scalar(@{$pdata->{raw}->{$section}}) <= 2)
    {
        # $pdata->{$section} = {};
        delete $pdata->{$section};
        printf $log_fh "%d: No data found.\n", __LINE__ 
            if ($verbose >= MAXVERBOSE);
        return 0;
    }
    #
    shift @{$pdata->{raw}->{$section}};
    pop @{$pdata->{raw}->{$section}};
    #
    printf $log_fh "%d: Section Lines: %d\n", __LINE__, scalar(@{$pdata->{raw}->{$section}})
        if ($verbose >= MAXVERBOSE);
    #
    %{$pdata->{$section}->{data}} = 
        map { split /\s*=\s*/, $_, 2 } @{$pdata->{raw}->{$section}};
    printf $log_fh "%d: Number of Keys: %d\n", __LINE__, scalar(keys %{$pdata->{$section}->{data}})
        if ($verbose >= MAXVERBOSE);
    #
    $pdata->{found_data}->{$section} = TRUE;
    #
    return 1;
}
#
sub split_quoted_string
{
    my $rec = shift;
    #
    my $rec_len = length($rec);
    #
    my $istart = -1;
    my $iend = -1;
    my $in_string = 0;
    #
    my @tokens = ();
    my $token = "";
    #
    for (my $i=0; $i<$rec_len; $i++)
    {
        my $c = substr($rec, $i, 1);
        #
        if ($in_string == 1)
        {
            if ($c eq '"')
            {
                $in_string = 0;
            }
            else
            {
                $token .= $c;
            }
        }
        elsif ($c eq '"')
        {
            $in_string = 1;
        }
        elsif ($c eq ' ')
        {
            # printf $log_fh "Token ... <%s>\n", $token;
            push (@tokens, $token);
            $token = '';
        }
        else
        {
            $token .= $c;
        }
    }
    #
    if (length($token) > 0)
    {
        # printf $log_fh "Token ... <%s>\n", $token;
        push (@tokens, $token);
        $token = '';
    }
    else
    {
        # push null-length token
        $token = '';
        push (@tokens, $token);
    }
    #
    # printf $log_fh "Tokens: \n%s\n", join("\n",@tokens);
    #
    return @tokens;
}
#
sub load_list
{
    my ($pdata, $section) = @_;
    #
    printf $log_fh "\n%d: Loading List Section: %s\n", __LINE__, $section
        if ($verbose >= MAXVERBOSE);
    #
    $pdata->{found_data}->{$section} = FALSE;
    #
    my $re_section = '\\' . $section;
    @{$pdata->{raw}->{$section}} = 
        grep /^${re_section}\s*$/ .. /^\s*$/, @{$pdata->{data}};
    #
    # printf $log_fh "<%s>\n", join("\n", @{$pdata->{raw}->{$section}});
    #
    if (scalar(@{$pdata->{raw}->{$section}}) <= 3)
    {
        # $pdata->{$section} = {};
        delete $pdata->{$section};
        printf $log_fh "%d: No data found.\n", __LINE__
            if ($verbose >= MAXVERBOSE);
        return 0;
    }
    shift @{$pdata->{raw}->{$section}};
    pop @{$pdata->{raw}->{$section}};
    $pdata->{$section}->{header} = shift @{$pdata->{raw}->{$section}};
    @{$pdata->{$section}->{column_names}} = 
        split / /, $pdata->{$section}->{header};
    my $number_columns = scalar(@{$pdata->{$section}->{column_names}});
    #
    @{$pdata->{$section}->{data}} = ();
    #
    printf $log_fh "%d: Section Lines: %d\n", __LINE__, scalar(@{$pdata->{raw}->{$section}})
        if ($verbose >= MAXVERBOSE);
    # printf $log_fh "Column Names: %d\n", $number_columns;
    foreach my $record (@{$pdata->{raw}->{$section}})
    {
        # printf $log_fh "\nRECORD: %s\n", $record;
        #
        # printf $log_fh "\nRECORD (original): %s\n", $record;
        # $record =~ s/"\s+"\s/"" /g;
        # $record =~ s/"\s+"\s*$/""/g;
        # printf $log_fh "\nRECORD (final): %s\n", $record;
        # my @tokens = split / /, $record;
        #
        my @tokens = split_quoted_string($record);
        my $number_tokens = scalar(@tokens);
        printf $log_fh "%d: Number of tokens in record: %d\n", __LINE__, $number_tokens
            if ($verbose >= MAXVERBOSE);
        #
        if ($number_tokens == $number_columns)
        {
            my %data = ();
            @data{@{$pdata->{$section}->{column_names}}} = @tokens;
            my $data_size = scalar(keys %data);
            # printf $log_fh "Current Data Size: %d\n", $data_size;
            unshift @{$pdata->{$section}->{data}}, \%data;
            printf $log_fh "%d: Current Number of Records: %d\n", __LINE__, scalar(@{$pdata->{$section}->{data}})
                if ($verbose >= MAXVERBOSE);
        }
        else
        {
            printf $log_fh "%d: SKIPPING RECORD - NUMBER TOKENS (%d) != NUMBER COLUMNS (%d)\n", __LINE__, $number_tokens, $number_columns;
        }
    }
    #
    $pdata->{found_data}->{$section} = TRUE;
    #
    return 1;
}
#
sub backfill_list
{
    my ($pdata, $section, $pcols) = @_;
    #
    foreach my $prow (@{$pdata->{$section}->{data}})
    {
        foreach my $col (@{$pcols})
        {
            # $prow->{$col} = 0 unless (defined($prow->{$col}));
            if (( ! exists($prow->{$col})) ||
                ( ! defined($prow->{$col})))
            {
                # printf "%d: WARNING - assigning ZERO to undefined column %s %s\n", __LINE__, $section, $col;
                $prow->{$col} = 0;
            }
        }
    }
}
#
########################################################################
########################################################################
#
# process U01 files.
#
sub export_u01_count_data
{
    my ($pdb) = @_;
    #
    ###############################################################
    #
    my $section = COUNT;
    #
    printf $log_fh "\n%d: Export Total Data For %s:\n", __LINE__, $section;
    #
    my $first_time = TRUE;
    #
    my $outfnm = "${export_dir}/COUNT_TOTALS.csv";
    printf $log_fh "%d: File %s already exists\n", __LINE__, $outfnm if ( -e $outfnm);
    open(my $outfh, ">" , $outfnm) || die $!;
    #
    foreach my $key (sort keys %{$totals{$section}{totals}})
    {
        if ($first_time == TRUE)
        {
            printf $outfh "%s", $key;
            $first_time = FALSE;
        }
        else
        {
            printf $outfh ",%s", $key;
        }
    }
    printf $outfh "\n";
    #
    $first_time = TRUE;
    foreach my $key (sort keys %{$totals{$section}{totals}})
    {
        if ($first_time == TRUE)
        {
            printf $outfh "%d", $totals{$section}{totals}{$key};
            $first_time = FALSE;
        }
        else
        {
            printf $outfh ",%d", $totals{$section}{totals}{$key};
        }
    }
    printf $outfh "\n";
    close($outfh);
    #
    $section = COUNT;
    #
    printf $log_fh "\n%d: Export Data For %s by Machine:\n", __LINE__, $section;
    #
    $first_time = TRUE;
    #
    $outfnm = "${export_dir}/COUNT_BY_MACHINE.csv";
    printf $log_fh "%d: File %s already exists\n", __LINE__, $outfnm if ( -e $outfnm);
    open($outfh, ">" , $outfnm) || die $!;
    foreach my $machine (sort { $a <=> $b } keys %{$totals{$section}{by_machine}})
    {
        if ($first_time == TRUE)
        {
            printf $outfh "machine";
            foreach my $key (sort keys %{$totals{$section}{by_machine}{$machine}})
            {
                printf $outfh ",%s", $key;
            }
            printf $outfh "\n";
            $first_time = FALSE;
        }
        #
        printf $outfh "%s", $machine;
        foreach my $key (sort keys %{$totals{$section}{by_machine}{$machine}})
        {
            printf $outfh ",%d", $totals{$section}{by_machine}{$machine}{$key};
        }
        printf $outfh "\n";
    }
    close($outfh);
    #
    $section = COUNT;
    #
    printf $log_fh "\n%d: Export Data For %s by Machine and Lane:\n", __LINE__, $section;
    #
    $first_time = TRUE;
    $outfnm = "${export_dir}/COUNT_BY_MACHINE_LANE.csv";
    printf $log_fh "%d: File %s already exists\n", __LINE__, $outfnm if ( -e $outfnm);
    open($outfh, ">" , $outfnm) || die $!;
    foreach my $machine (sort { $a <=> $b } keys %{$totals{$section}{by_machine_lane}})
    {
        foreach my $lane (sort { $a <=> $b } keys %{$totals{$section}{by_machine_lane}{$machine}})
        {
            if ($first_time == TRUE)
            {
                printf $outfh "machine,lane";
                foreach my $key (sort keys %{$totals{$section}{by_machine_lane}{$machine}{$lane}})
                {
                    printf $outfh ",%s", $key;
                }
                printf $outfh "\n";
                $first_time = FALSE;
            }
            #
            printf $outfh "%s,%s", $machine, $lane;
            foreach my $key (sort keys %{$totals{$section}{by_machine_lane}{$machine}{$lane}})
            {
                printf $outfh ",%d", $totals{$section}{by_machine_lane}{$machine}{$lane}{$key};
            }
            printf $outfh "\n";
        }
    }
    close($outfh);
    #
    ###############################################################
    #
    $section = COUNT;
    #
    printf $log_fh "\n%d: Export Total Data For %s by Product:\n", __LINE__, $section;
    #
    $first_time = TRUE;
    $outfnm = "${export_dir}/COUNT_TOTALS_BY_PRODUCT.csv";
    printf $log_fh "%d: File %s already exists\n", __LINE__, $outfnm if ( -e $outfnm);
    open($outfh, ">" , $outfnm) || die $!;
    foreach my $product (sort keys %{$totals{$section}{by_product}})
    {
        if ($first_time == TRUE)
        {
            printf $outfh "product";
            foreach my $key (sort keys %{$totals{$section}{by_product}{$product}{totals}})
            {
                printf $outfh ",%s", $key;
            }
            printf $outfh "\n";
            $first_time = FALSE;
        }
        #
        printf $outfh "%s", $product;
        foreach my $key (sort keys %{$totals{$section}{by_product}{$product}{totals}})
        {
            printf $outfh ",%d", $totals{$section}{by_product}{$product}{totals}{$key};
        }
        printf $outfh "\n";
    }
    close($outfh);
    #
    $section = COUNT;
    #
    printf $log_fh "\n%d: Export Data For %s by Product and Machine:\n", __LINE__, $section;
    #
    $first_time = TRUE;
    $outfnm = "${export_dir}/COUNT_BY_PRODUCT_MACHINE.csv";
    printf $log_fh "%d: File %s already exists\n", __LINE__, $outfnm if ( -e $outfnm);
    open($outfh, ">" , $outfnm) || die $!;
    foreach my $product (sort keys %{$totals{$section}{by_product}})
    {
        foreach my $machine (sort { $a <=> $b } keys %{$totals{$section}{by_product}{$product}{by_machine}})
        {
            if ($first_time == TRUE)
            {
                printf $outfh "product,machine";
                foreach my $key (sort keys %{$totals{$section}{by_product}{$product}{by_machine}{$machine}})
                {
                    printf $outfh ",%s", $key;
                }
                printf $outfh "\n";
                $first_time = FALSE;
            }
            #
            printf $outfh "%s,%s", $product, $machine;
            foreach my $key (sort keys %{$totals{$section}{by_product}{$product}{by_machine}{$machine}})
            {
                printf $outfh ",%d", $totals{$section}{by_product}{$product}{by_machine}{$machine}{$key};
            }
            printf $outfh "\n";
        }
    }
    close($outfh);
    #
    $section = COUNT;
    #
    printf $log_fh "\n%d: Export Data For %s by Product, Machine and Lane:\n", __LINE__, $section;
    #
    $first_time = TRUE;
    $outfnm = "${export_dir}/COUNT_BY_PRODUCT_MACHINE_LANE.csv";
    printf $log_fh "%d: File %s already exists\n", __LINE__, $outfnm if ( -e $outfnm);
    open($outfh, ">" , $outfnm) || die $!;
    foreach my $product (sort keys %{$totals{$section}{by_product}})
    {
        foreach my $machine (sort { $a <=> $b } keys %{$totals{$section}{by_product}{$product}{by_machine_lane}})
        {
            foreach my $lane (sort { $a <=> $b } keys %{$totals{$section}{by_product}{$product}{by_machine_lane}{$machine}})
            {
                if ($first_time == TRUE)
                {
                    printf $outfh "product,machine,lane";
                    foreach my $key (sort keys %{$totals{$section}{by_product}{$product}{by_machine_lane}{$machine}{$lane}})
                    {
                        printf $outfh ",%s", $key;
                    }
                    printf $outfh "\n";
                    $first_time = FALSE;
                }
                #
                printf $outfh "%s,%s,%s", $product, $machine, $lane;
                foreach my $key (sort keys %{$totals{$section}{by_product}{$product}{by_machine_lane}{$machine}{$lane}})
                {
                    printf $outfh ",%d", $totals{$section}{by_product}{$product}{by_machine_lane}{$machine}{$lane}{$key};
                }
                printf $outfh "\n";
            }
        }
    }
    close($outfh);
}
#
sub export_u01_time_data
{
    my ($pdb) = @_;
    #
    ###############################################################
    #
    my $section = TIME;
    #
    printf $log_fh "\n%d: Export Total Data For %s:\n", __LINE__, $section;
    #
    my $first_time = TRUE;
    #
    my $outfnm = "${export_dir}/TIME_TOTALS.csv";
    printf $log_fh "%d: File %s already exists\n", __LINE__, $outfnm if ( -e $outfnm);
    open(my $outfh, ">" , $outfnm) || die $!;
    #
    foreach my $key (sort keys %{$totals{$section}{totals}})
    {
        if ($first_time == TRUE)
        {
            printf $outfh "%s", $key;
            $first_time = FALSE;
        }
        else
        {
            printf $outfh ",%s", $key;
        }
    }
    printf $outfh "\n";
    #
    $first_time = TRUE;
    foreach my $key (sort keys %{$totals{$section}{totals}})
    {
        if ($first_time == TRUE)
        {
            printf $outfh "%s", $totals{$section}{totals}{$key};
            $first_time = FALSE;
        }
        else
        {
            printf $outfh ",%s", $totals{$section}{totals}{$key};
        }
    }
    printf $outfh "\n";
    close($outfh);
    #
    ###############################################################
    #
    $section = TIME;
    #
    printf $log_fh "\n%d: Export Data For %s by Machine:\n", __LINE__, $section;
    #
    $first_time = TRUE;
    #
    $outfnm = "${export_dir}/TIME_BY_MACHINE.csv";
    printf $log_fh "%d: File %s already exists\n", __LINE__, $outfnm if ( -e $outfnm);
    open($outfh, ">" , $outfnm) || die $!;
    foreach my $machine (sort { $a <=> $b } keys %{$totals{$section}{by_machine}})
    {
        if ($first_time == TRUE)
        {
            printf $outfh "machine";
            foreach my $key (sort keys %{$totals{$section}{by_machine}{$machine}})
            {
                printf $outfh ",%s", $key;
            }
            printf $outfh "\n";
            $first_time = FALSE;
        }
        #
        printf $outfh "%s", $machine;
        foreach my $key (sort keys %{$totals{$section}{by_machine}{$machine}})
        {
            printf $outfh ",%s", $totals{$section}{by_machine}{$machine}{$key};
        }
        printf $outfh "\n", 
    }
    close($outfh);
    #
    $section = TIME;
    #
    printf $log_fh "\n%d: Export Data For %s by Machine and Lane:\n", __LINE__, $section;
    #
    $first_time = TRUE;
    $outfnm = "${export_dir}/TIME_BY_MACHINE_LANE.csv";
    printf $log_fh "%d: File %s already exists\n", __LINE__, $outfnm if ( -e $outfnm);
    open($outfh, ">" , $outfnm) || die $!;
    foreach my $machine (sort { $a <=> $b } keys %{$totals{$section}{by_machine_lane}})
    {
        foreach my $lane (sort { $a <=> $b } keys %{$totals{$section}{by_machine_lane}{$machine}})
        {
            if ($first_time == TRUE)
            {
                printf $outfh "machine,lane";
                foreach my $key (sort keys %{$totals{$section}{by_machine_lane}{$machine}{$lane}})
                {
                    printf $outfh ",%s", $key;
                }
                printf $outfh "\n";
                $first_time = FALSE;
            }
            #
            printf $outfh "%s,%s", $machine, $lane;
            foreach my $key (sort keys %{$totals{$section}{by_machine_lane}{$machine}{$lane}})
            {
                printf $outfh ",%s", $totals{$section}{by_machine_lane}{$machine}{$lane}{$key};
            }
            printf $outfh "\n";
        }
    }
    close($outfh);
    #
    ###############################################################
    #
    $section = TIME;
    #
    printf $log_fh "\n%d: Export Total Data For %s by Product:\n", __LINE__, $section;
    #
    $first_time = TRUE;
    $outfnm = "${export_dir}/TIME_TOTALS_BY_PRODUCT.csv";
    printf $log_fh "%d: File %s already exists\n", __LINE__, $outfnm if ( -e $outfnm);
    open($outfh, ">" , $outfnm) || die $!;
    foreach my $product (sort keys %{$totals{$section}{by_product}})
    {
        if ($first_time == TRUE)
        {
            printf $outfh "product";
            foreach my $key (sort keys %{$totals{$section}{by_product}{$product}{totals}})
            {
                printf $outfh ",%s", $key;
            }
            printf $outfh "\n";
            $first_time = FALSE;
        }
        #
        printf $outfh "%s", $product;
        foreach my $key (sort keys %{$totals{$section}{by_product}{$product}{totals}})
        {
            printf $outfh ",%s", $totals{$section}{by_product}{$product}{totals}{$key};
        }
        printf $outfh "\n";
    }
    close($outfh);
    #
    ###############################################################
    #
    $section = TIME;
    #
    printf $log_fh "\n%d: Export Data For %s by Product and Machine:\n", __LINE__, $section;
    #
    $first_time = TRUE;
    $outfnm = "${export_dir}/TIME_BY_PRODUCT_MACHINE.csv";
    printf $log_fh "%d: File %s already exists\n", __LINE__, $outfnm if ( -e $outfnm);
    open($outfh, ">" , $outfnm) || die $!;
    foreach my $product (sort keys %{$totals{$section}{by_product}})
    {
        foreach my $machine (sort { $a <=> $b } keys %{$totals{$section}{by_product}{$product}{by_machine}})
        {
            if ($first_time == TRUE)
            {
                printf $outfh "product,machine";
                foreach my $key (sort keys %{$totals{$section}{by_product}{$product}{by_machine}{$machine}})
                {
                    printf $outfh ",%s", $key;
                }
                printf $outfh "\n";
                $first_time = FALSE;
            }
            #
            printf $outfh "%s,%s", $product, $machine;
            foreach my $key (sort keys %{$totals{$section}{by_product}{$product}{by_machine}{$machine}})
            {
                printf $outfh ",%s", $totals{$section}{by_product}{$product}{by_machine}{$machine}{$key};
            }
            printf $outfh "\n";
        }
    }
    close($outfh);
    #
    printf $log_fh "\n%d: Export Data For %s by Product, Machine and Lane:\n", __LINE__, $section;
    #
    $first_time = TRUE;
    $outfnm = "${export_dir}/TIME_BY_PRODUCT_MACHINE_LANE.csv";
    printf $log_fh "%d: File %s already exists\n", __LINE__, $outfnm if ( -e $outfnm);
    open($outfh, ">" , $outfnm) || die $!;
    foreach my $product (sort keys %{$totals{$section}{by_product}})
    {
        foreach my $machine (sort { $a <=> $b } keys %{$totals{$section}{by_product}{$product}{by_machine_lane}})
        {
            foreach my $lane (sort { $a <=> $b } keys %{$totals{$section}{by_product}{$product}{by_machine_lane}{$machine}})
            {
                if ($first_time == TRUE)
                {
                    printf $outfh "product,machine,lane";
                    foreach my $key (sort keys %{$totals{$section}{by_product}{$product}{by_machine_lane}{$machine}{$lane}})
                    {
                        printf $outfh ",%s", $key;
                    }
                    printf $outfh "\n";
                    $first_time = FALSE;
                }
                #
                printf $outfh "%s,s,%s", $product, $machine, $lane;
                foreach my $key (sort keys %{$totals{$section}{by_product}{$product}{by_machine_lane}{$machine}{$lane}})
                {
                    printf $outfh ",%s", $totals{$section}{by_product}{$product}{by_machine_lane}{$machine}{$lane}{$key};
                }
                printf $outfh "\n";
            }
        }
    }
    close($outfh);
}
#
sub export_u01_feeder_data
{
    my ($pdb) = @_;
    #
    ###############################################################
    #
    my $section = MOUNTPICKUPFEEDER;
    #
    printf $log_fh "\n%d: Export Data For %s by Machine, Lane, Stage, FAdd, FSAdd, ReelID:\n", __LINE__, $section;
    #
    my $outfnm = "${export_dir}/FEEDER_BY_MACHINE_LANE_STAGE_FADD_FSADD_REELID.csv";
    printf $log_fh "%d: File %s already exists\n", __LINE__, $outfnm if ( -e $outfnm);
    open(my $outfh, ">" , $outfnm) || die $!;
    foreach my $pcol (@feeder_export_cols)
    {
        printf $outfh $pcol->{format}, $pcol->{name};
    }
    printf $outfh "\n";
    #
    foreach my $machine (sort { $a <=> $b } keys %{$totals{$section}{by_machine_lane_stage_fadd_fsadd_reelid}})
    {
        foreach my $lane (sort { $a <=> $b } keys %{$totals{$section}{by_machine_lane_stage_fadd_fsadd_reelid}{$machine}})
        {
            foreach my $stage (sort { $a <=> $b } keys %{$totals{$section}{by_machine_lane_stage_fadd_fsadd_reelid}{$machine}{$lane}})
            {
                foreach my $fadd (sort { $a <=> $b } keys %{$totals{$section}{by_machine_lane_stage_fadd_fsadd_reelid}{$machine}{$lane}{$stage}})
                {
                    foreach my $fsadd (sort { $a <=> $b } keys %{$totals{$section}{by_machine_lane_stage_fadd_fsadd_reelid}{$machine}{$lane}{$stage}{$fadd}})
                    {
                        foreach my $reelid (sort keys %{$totals{$section}{by_machine_lane_stage_fadd_fsadd_reelid}{$machine}{$lane}{$stage}{$fadd}{$fsadd}})
                        {
                            printf $outfh "%s,%s,%s,%s,%s,%s", $machine, $lane, $stage, $fadd, $fsadd, $reelid;
                            foreach my $col (@feeder_count_cols)
                            {
                                printf $outfh ",%d", $totals{$section}{by_machine_lane_stage_fadd_fsadd_reelid}{$machine}{$lane}{$stage}{$fadd}{$fsadd}{$reelid}{$col};
                            }
                            printf $outfh "\n";
                        }
                    }
                }
            }
        }
    }
    close($outfh);
    #
    printf $log_fh "\n%d: Export Data For %s by Machine, Lane, Stage, FAdd, FSAdd:\n", __LINE__, $section;
    #
    $outfnm = "${export_dir}/FEEDER_BY_MACHINE_LANE_STAGE_FADD_FSADD.csv";
    printf $log_fh "%d: File %s already exists\n", __LINE__, $outfnm if ( -e $outfnm);
    open($outfh, ">" , $outfnm) || die $!;
    foreach my $pcol (@feeder_export_cols2)
    {
        printf $outfh $pcol->{format}, $pcol->{name};
    }
    printf $outfh "\n";
    #
    foreach my $machine (sort { $a <=> $b } keys %{$totals{$section}{by_machine_lane_stage_fadd_fsadd}})
    {
        foreach my $lane (sort { $a <=> $b } keys %{$totals{$section}{by_machine_lane_stage_fadd_fsadd}{$machine}})
        {
            foreach my $stage (sort { $a <=> $b } keys %{$totals{$section}{by_machine_lane_stage_fadd_fsadd}{$machine}{$lane}})
            {
                foreach my $fadd (sort { $a <=> $b } keys %{$totals{$section}{by_machine_lane_stage_fadd_fsadd}{$machine}{$lane}{$stage}})
                {
                    foreach my $fsadd (sort { $a <=> $b } keys %{$totals{$section}{by_machine_lane_stage_fadd_fsadd}{$machine}{$lane}{$stage}{$fadd}})
                    {
                        printf $outfh "%s,%s,%s,%s,%s", $machine, $lane, $stage, $fadd, $fsadd;
                        foreach my $col (@feeder_count_cols)
                        {
                            printf $outfh ",%d", $totals{$section}{by_machine_lane_stage_fadd_fsadd}{$machine}{$lane}{$stage}{$fadd}{$fsadd}{$col};
                        }
                        printf $outfh "\n";
                    }
                }
            }
        }
    }
    close($outfh);
    #
    printf $log_fh "\n%d: Export Data For %s by Machine, Lane, Stage, TableNo:\n", __LINE__, $section;
    #
    $outfnm = "${export_dir}/FEEDER_BY_MACHINE_LANE_STAGE_TABLE_NO.csv";
    printf $log_fh "%d: File %s already exists\n", __LINE__, $outfnm if ( -e $outfnm);
    open($outfh, ">" , $outfnm) || die $!;
    foreach my $pcol (@feeder_export_cols3)
    {
        printf $outfh $pcol->{format}, $pcol->{name};
    }
    printf $outfh "\n";
    #
    foreach my $machine (sort { $a <=> $b } keys %{$totals{$section}{by_machine_lane_stage_table_no}})
    {
        foreach my $lane (sort { $a <=> $b } keys %{$totals{$section}{by_machine_lane_stage_table_no}{$machine}})
        {
            foreach my $stage (sort { $a <=> $b } keys %{$totals{$section}{by_machine_lane_stage_table_no}{$machine}{$lane}})
            {
                foreach my $table_no (sort { $a <=> $b } keys %{$totals{$section}{by_machine_lane_stage_table_no}{$machine}{$lane}{$stage}})
                {
                    printf $outfh "%s,%s,%s,%s", $machine, $lane, $stage, $table_no;
                    foreach my $col (@feeder_count_cols)
                    {
                        printf $outfh ",%d", $totals{$section}{by_machine_lane_stage_table_no}{$machine}{$lane}{$stage}{$table_no}{$col};
                    }
                    printf $outfh "\n";
                }
            }
        }
    }
    close($outfh);
    #
    ###############################################################
    #
    $section = MOUNTPICKUPFEEDER;
    #
    printf $log_fh "\n%d: Export Data For %s by Product, Machine, Lane, Stage, FAdd, FSAdd, ReelID:\n", __LINE__, $section;
    #
    $outfnm = "${export_dir}/FEEDER_BY_PRODUCT_MACHINE_LANE_STAGE_FADD_FSADD_REELID.csv";
    printf $log_fh "%d: File %s already exists\n", __LINE__, $outfnm if ( -e $outfnm);
    open($outfh, ">" , $outfnm) || die $!;
    printf $outfh "product,";
    foreach my $pcol (@feeder_export_cols)
    {
        printf $outfh $pcol->{format}, $pcol->{name};
    }
    printf $outfh "\n";
    #
    foreach my $product (sort keys %{$totals{$section}{by_product}})
    {
        foreach my $machine (sort { $a <=> $b } keys %{$totals{$section}{by_product}{$product}{by_machine_lane_stage_fadd_fsadd_reelid}})
        {
            foreach my $lane (sort { $a <=> $b } keys %{$totals{$section}{by_product}{$product}{by_machine_lane_stage_fadd_fsadd_reelid}{$machine}})
            {
                foreach my $stage (sort { $a <=> $b } keys %{$totals{$section}{by_product}{$product}{by_machine_lane_stage_fadd_fsadd_reelid}{$machine}{$lane}})
                {
                    foreach my $fadd (sort { $a <=> $b } keys %{$totals{$section}{by_product}{$product}{by_machine_lane_stage_fadd_fsadd_reelid}{$machine}{$lane}{$stage}})
                    {
                        foreach my $fsadd (sort { $a <=> $b } keys %{$totals{$section}{by_product}{$product}{by_machine_lane_stage_fadd_fsadd_reelid}{$machine}{$lane}{$stage}{$fadd}})
                        {
                            foreach my $reelid (sort keys %{$totals{$section}{by_product}{$product}{by_machine_lane_stage_fadd_fsadd_reelid}{$machine}{$lane}{$stage}{$fadd}{$fsadd}})
                            {
                                printf $outfh "%s,%s,%s,%s,%s,%s,%s", $product, $machine, $lane, $stage, $fadd, $fsadd, $reelid;
                                foreach my $col (@feeder_count_cols)
                                {
                                    printf $outfh ",%d", $totals{$section}{by_product}{$product}{by_machine_lane_stage_fadd_fsadd_reelid}{$machine}{$lane}{$stage}{$fadd}{$fsadd}{$reelid}{$col};
                                }
                                printf $outfh "\n";
                            }
                        }
                    }
                }
            }
        }
    }
    close($outfh);
    #
    printf $log_fh "\n%d: Export Data For %s by Product, Machine, Lane, Stage, FAdd, FSAdd:\n", __LINE__, $section;
    #
    $outfnm = "${export_dir}/FEEDER_BY_PRODUCT_MACHINE_LANE_STAGE_FADD_FSADD.csv";
    printf $log_fh "%d: File %s already exists\n", __LINE__, $outfnm if ( -e $outfnm);
    open($outfh, ">" , $outfnm) || die $!;
    printf $outfh "product,";
    foreach my $pcol (@feeder_export_cols2)
    {
        printf $outfh $pcol->{format}, $pcol->{name};
    }
    printf $outfh "\n";
    #
    foreach my $product (sort keys %{$totals{$section}{by_product}})
    {
        foreach my $machine (sort { $a <=> $b } keys %{$totals{$section}{by_product}{$product}{by_machine_lane_stage_fadd_fsadd}})
        {
            foreach my $lane (sort { $a <=> $b } keys %{$totals{$section}{by_product}{$product}{by_machine_lane_stage_fadd_fsadd}{$machine}})
            {
                foreach my $stage (sort { $a <=> $b } keys %{$totals{$section}{by_product}{$product}{by_machine_lane_stage_fadd_fsadd}{$machine}{$lane}})
                {
                    foreach my $fadd (sort { $a <=> $b } keys %{$totals{$section}{by_product}{$product}{by_machine_lane_stage_fadd_fsadd}{$machine}{$lane}{$stage}})
                    {
                        foreach my $fsadd (sort { $a <=> $b } keys %{$totals{$section}{by_product}{$product}{by_machine_lane_stage_fadd_fsadd}{$machine}{$lane}{$stage}{$fadd}})
                        {
                            printf $outfh "%s,%s,%s,%s,%s,%s", $product, $machine, $lane, $stage, $fadd, $fsadd;
                            foreach my $col (@feeder_count_cols)
                            {
                                printf $outfh ",%d", $totals{$section}{by_product}{$product}{by_machine_lane_stage_fadd_fsadd}{$machine}{$lane}{$stage}{$fadd}{$fsadd}{$col};
                            }
                            printf $outfh "\n";
                        }
                    }
                }
            }
        }
    }
    close($outfh);
    #
    printf $log_fh "\n%d: Export Data For %s by Product, Machine, Lane, Stage, TableNo:\n", __LINE__, $section;
    #
    $outfnm = "${export_dir}/FEEDER_BY_PRODUCT_MACHINE_LANE_STAGE_TABLE_NO.csv";
    printf $log_fh "%d: File %s already exists\n", __LINE__, $outfnm if ( -e $outfnm);
    open($outfh, ">" , $outfnm) || die $!;
    printf $outfh "product,";
    foreach my $pcol (@feeder_export_cols3)
    {
        printf $outfh $pcol->{format}, $pcol->{name};
    }
    printf $outfh "\n";
    #
    foreach my $product (sort keys %{$totals{$section}{by_product}})
    {
        foreach my $machine (sort { $a <=> $b } keys %{$totals{$section}{by_product}{$product}{by_machine_lane_stage_table_no}})
        {
            foreach my $lane (sort { $a <=> $b } keys %{$totals{$section}{by_product}{$product}{by_machine_lane_stage_table_no}{$machine}})
            {
                foreach my $stage (sort { $a <=> $b } keys %{$totals{$section}{by_product}{$product}{by_machine_lane_stage_table_no}{$machine}{$lane}})
                {
                    foreach my $table_no (sort { $a <=> $b } keys %{$totals{$section}{by_product}{$product}{by_machine_lane_stage_table_no}{$machine}{$lane}{$stage}})
                    {
                        printf $outfh "%s,%s,%s,%s,%s", $product, $machine, $lane, $stage, $table_no;
                        foreach my $col (@feeder_count_cols)
                        {
                            printf $outfh ",%d", $totals{$section}{by_product}{$product}{by_machine_lane_stage_table_no}{$machine}{$lane}{$stage}{$table_no}{$col};
                        }
                        printf $outfh "\n";
                    }
                }
            }
        }
    }
    close($outfh);
}
#
sub export_u01_nozzle_data
{
    my ($pdb) = @_;
    #
    ###############################################################
    #
    my $section = MOUNTPICKUPNOZZLE;
    #
    printf $log_fh "\n%d: Export Data For %s by Machine, Lane, Stage, NHAdd, NCAdd, Blkserial:\n", __LINE__, $section;
    #
    my $outfnm = "${export_dir}/NOZZLE_BY_MACHINE_LANE_STAGE_NHADD_NCADD_BLKSERIAL.csv";
    printf $log_fh "%d: File %s already exists\n", __LINE__, $outfnm if ( -e $outfnm);
    open(my $outfh, ">" , $outfnm) || die $!;
    foreach my $pcol (@nozzle_export_cols)
    {
        printf $outfh $pcol->{format}, $pcol->{name};
    }
    printf $outfh "\n";
    #
    foreach my $machine (sort { $a <=> $b } keys %{$totals{$section}{by_machine_lane_stage_nhadd_ncadd_blkserial}})
    {
        foreach my $lane (sort { $a <=> $b } keys %{$totals{$section}{by_machine_lane_stage_nhadd_ncadd_blkserial}{$machine}})
        {
            foreach my $stage (sort { $a <=> $b } keys %{$totals{$section}{by_machine_lane_stage_nhadd_ncadd_blkserial}{$machine}{$lane}})
            {
                foreach my $nhadd (sort { $a <=> $b } keys %{$totals{$section}{by_machine_lane_stage_nhadd_ncadd_blkserial}{$machine}{$lane}{$stage}})
                {
                    foreach my $ncadd (sort { $a <=> $b } keys %{$totals{$section}{by_machine_lane_stage_nhadd_ncadd_blkserial}{$machine}{$lane}{$stage}{$nhadd}})
                    {
                        foreach my $blkserial (sort keys %{$totals{$section}{by_machine_lane_stage_nhadd_ncadd_blkserial}{$machine}{$lane}{$stage}{$nhadd}{$ncadd}})
                        {
                            printf $outfh "%s,%s,%s,%s,%s,%s",
                                $machine, $lane, $stage, $nhadd, $ncadd, $blkserial;
                            foreach my $col (@nozzle_count_cols)
                            {
                                printf $outfh ",%d", 
                                    $totals{$section}{by_machine_lane_stage_nhadd_ncadd_blkserial}{$machine}{$lane}{$stage}{$nhadd}{$ncadd}{$blkserial}{$col};
                            }
                            printf $outfh "\n";
                        }
                    }
                }
            }
        }
    }
    close($outfh);
    #
    printf $log_fh "\n%d: Export Data For %s by Machine, Lane, Stage, NHAdd, NCAdd:\n", __LINE__, $section;
    #
    $outfnm = "${export_dir}/NOZZLE_BY_MACHINE_LANE_STAGE_NHADD_NCADD.csv";
    printf $log_fh "%d: File %s already exists\n", __LINE__, $outfnm if ( -e $outfnm);
    open($outfh, ">" , $outfnm) || die $!;
    foreach my $pcol (@nozzle_export_cols2)
    {
        printf $outfh $pcol->{format}, $pcol->{name};
    }
    printf $outfh "\n";
    #
    foreach my $machine (sort { $a <=> $b } keys %{$totals{$section}{by_machine_lane_stage_nhadd_ncadd}})
    {
        foreach my $lane (sort { $a <=> $b } keys %{$totals{$section}{by_machine_lane_stage_nhadd_ncadd}{$machine}})
        {
            foreach my $stage (sort { $a <=> $b } keys %{$totals{$section}{by_machine_lane_stage_nhadd_ncadd}{$machine}{$lane}})
            {
                foreach my $nhadd (sort { $a <=> $b } keys %{$totals{$section}{by_machine_lane_stage_nhadd_ncadd}{$machine}{$lane}{$stage}})
                {
                    foreach my $ncadd (sort { $a <=> $b } keys %{$totals{$section}{by_machine_lane_stage_nhadd_ncadd}{$machine}{$lane}{$stage}{$nhadd}})
                    {
                        printf $outfh "%s,%s,%s,%s,%s", $machine, $lane, $stage, $nhadd, $ncadd;
                        foreach my $col (@nozzle_count_cols)
                        {
                            printf $outfh ",%d", $totals{$section}{by_machine_lane_stage_nhadd_ncadd}{$machine}{$lane}{$stage}{$nhadd}{$ncadd}{$col};
                        }
                        printf $outfh "\n";
                    }
                }
            }
        }
    }
    close($outfh);
    #
    ###############################################################
    #
    $section = MOUNTPICKUPNOZZLE;
    #
    printf $log_fh "\n%d: Export Data For %s by Product, Machine, Lane, Stage, NHAdd, NCAdd, Blkserial:\n", __LINE__, $section;
    #
    $outfnm = "${export_dir}/NOZZLE_BY_PRODUCT_MACHINE_LANE_STAGE_NHADD_NCADD_BLKSERIAL.csv";
    printf $log_fh "%d: File %s already exists\n", __LINE__, $outfnm if ( -e $outfnm);
    open($outfh, ">" , $outfnm) || die $!;
    printf $outfh "product,";
    foreach my $pcol (@nozzle_export_cols)
    {
        printf $outfh $pcol->{format}, $pcol->{name};
    }
    printf $outfh "\n";
    #
    foreach my $product (sort keys %{$totals{$section}{by_product}})
    {
        foreach my $machine (sort { $a <=> $b } keys %{$totals{$section}{by_product}{$product}{by_machine_lane_stage_nhadd_ncadd_blkserial}})
        {
            foreach my $lane (sort { $a <=> $b } keys %{$totals{$section}{by_product}{$product}{by_machine_lane_stage_nhadd_ncadd_blkserial}{$machine}})
            {
                foreach my $stage (sort { $a <=> $b } keys %{$totals{$section}{by_product}{$product}{by_machine_lane_stage_nhadd_ncadd_blkserial}{$machine}{$lane}})
                {
                    foreach my $nhadd (sort { $a <=> $b } keys %{$totals{$section}{by_product}{$product}{by_machine_lane_stage_nhadd_ncadd_blkserial}{$machine}{$lane}{$stage}})
                    {
                        foreach my $ncadd (sort { $a <=> $b } keys %{$totals{$section}{by_product}{$product}{by_machine_lane_stage_nhadd_ncadd_blkserial}{$machine}{$lane}{$stage}{$nhadd}})
                        {
                            foreach my $blkserial (sort keys %{$totals{$section}{by_product}{$product}{by_machine_lane_stage_nhadd_ncadd_blkserial}{$machine}{$lane}{$stage}{$nhadd}{$ncadd}})
                            {
                                printf $outfh "%s,%s,%s,%s,%s,%s,%s", $product, $machine, $lane, $stage, $nhadd, $ncadd, $blkserial;
                                foreach my $col (@nozzle_count_cols)
                                {
                                    printf $outfh ",%d", $totals{$section}{by_product}{$product}{by_machine_lane_stage_nhadd_ncadd_blkserial}{$machine}{$lane}{$stage}{$nhadd}{$ncadd}{$blkserial}{$col};
                                }
                                printf $outfh "\n";
                            }
                        }
                    }
                }
            }
        }
    }
    close($outfh);
    #
    printf $log_fh "\n%d: Export Data For %s by Product, Machine, Lane, Stage, NHAdd, NCAdd:\n", __LINE__, $section;
    #
    $outfnm = "${export_dir}/NOZZLE_BY_PRODUCT_MACHINE_LANE_STAGE_NHADD_NCADD.csv";
    printf $log_fh "%d: File %s already exists\n", __LINE__, $outfnm if ( -e $outfnm);
    open($outfh, ">" , $outfnm) || die $!;
    printf $outfh "product,";
    foreach my $pcol (@nozzle_export_cols2)
    {
        printf $outfh $pcol->{format}, $pcol->{name};
    }
    printf $outfh "\n";
    #
    foreach my $product (sort keys %{$totals{$section}{by_product}})
    {
        foreach my $machine (sort { $a <=> $b } keys %{$totals{$section}{by_product}{$product}{by_machine_lane_stage_nhadd_ncadd}})
        {
            foreach my $lane (sort { $a <=> $b } keys %{$totals{$section}{by_product}{$product}{by_machine_lane_stage_nhadd_ncadd}{$machine}})
            {
                foreach my $stage (sort { $a <=> $b } keys %{$totals{$section}{by_product}{$product}{by_machine_lane_stage_nhadd_ncadd}{$machine}{$lane}})
                {
                    foreach my $nhadd (sort { $a <=> $b } keys %{$totals{$section}{by_product}{$product}{by_machine_lane_stage_nhadd_ncadd}{$machine}{$lane}{$stage}})
                    {
                        foreach my $ncadd (sort { $a <=> $b } keys %{$totals{$section}{by_product}{$product}{by_machine_lane_stage_nhadd_ncadd}{$machine}{$lane}{$stage}{$nhadd}})
                        {
                            printf $outfh "%s,%s,%s,%s,%s,%s",
                                $product, $machine, $lane, $stage, $nhadd, $ncadd;
                            foreach my $col (@nozzle_count_cols)
                            {
                                printf $outfh ",%d", $totals{$section}{by_product}{$product}{by_machine_lane_stage_nhadd_ncadd}{$machine}{$lane}{$stage}{$nhadd}{$ncadd}{$col};
                            }
                            printf $outfh "\n";
                        }
                    }
                }
            }
        }
    }
    close($outfh);
}
#
sub export_u01_nozzle_data_keys
{
    my ($pdb, $nmkey1, $nmkey2, $label) = @_;
    #
    my $NMKEY1 = $nmkey1;
    $NMKEY1 =~ tr/[a-z]/[A-Z]/;
    my $NMKEY2 = $nmkey2;
    $NMKEY2 =~ tr/[a-z]/[A-Z]/;
    my $LABEL = $label;
    $LABEL =~ tr/[a-z]/[A-Z]/;
    #
    ###############################################################
    #
    my $section = MOUNTPICKUPNOZZLE;
    #
    printf $log_fh "\n%d: Export Data For %s by Machine, Lane, Stage, $nmkey1, $nmkey2, Blkserial:\n", __LINE__, $section;
    #
    my $outfnm = "${export_dir}/NOZZLE_BY_MACHINE_LANE_STAGE_${NMKEY1}_${NMKEY2}_BLKSERIAL.csv";
    printf $log_fh "%d: File %s already exists\n", __LINE__, $outfnm if ( -e $outfnm);
    open(my $outfh, ">" , $outfnm) || die $!;
    foreach my $pcol (@{$nozzle_export_cols_new{$label}})
    {
        printf $outfh $pcol->{format}, $pcol->{name};
    }
    printf $outfh "\n";
    #
    foreach my $machine (sort { $a <=> $b } keys %{$totals{$section}{$label}{by_machine_lane_stage_key1_key2_blkserial}})
    {
        foreach my $lane (sort { $a <=> $b } keys %{$totals{$section}{$label}{by_machine_lane_stage_key1_key2_blkserial}{$machine}})
        {
            foreach my $stage (sort { $a <=> $b } keys %{$totals{$section}{$label}{by_machine_lane_stage_key1_key2_blkserial}{$machine}{$lane}})
            {
                foreach my $key1 (sort { $a <=> $b } keys %{$totals{$section}{$label}{by_machine_lane_stage_key1_key2_blkserial}{$machine}{$lane}{$stage}})
                {
                    foreach my $key2 (sort { $a <=> $b } keys %{$totals{$section}{$label}{by_machine_lane_stage_key1_key2_blkserial}{$machine}{$lane}{$stage}{$key1}})
                    {
                        foreach my $blkserial (sort keys %{$totals{$section}{$label}{by_machine_lane_stage_key1_key2_blkserial}{$machine}{$lane}{$stage}{$key1}{$key2}})
                        {
                            printf $outfh "%s,%s,%s,%s,%s,%s",
                                $machine, $lane, $stage, $key1, $key2, $blkserial;
                            foreach my $col (@nozzle_count_cols)
                            {
                                printf $outfh ",%d", 
                                    $totals{$section}{$label}{by_machine_lane_stage_key1_key2_blkserial}{$machine}{$lane}{$stage}{$key1}{$key2}{$blkserial}{$col};
                            }
                            printf $outfh "\n";
                        }
                    }
                }
            }
        }
    }
    close($outfh);
    #
    printf $log_fh "\n%d: Export Data For %s by Machine, Lane, Stage, $nmkey1, $nmkey2:\n", __LINE__, $section;
    #
    $outfnm = "${export_dir}/NOZZLE_BY_MACHINE_LANE_STAGE_${NMKEY1}_${NMKEY2}.csv";
    printf $log_fh "%d: File %s already exists\n", __LINE__, $outfnm if ( -e $outfnm);
    open($outfh, ">" , $outfnm) || die $!;
    foreach my $pcol (@{$nozzle_export_cols2_new{$label}})
    {
        printf $outfh $pcol->{format}, $pcol->{name};
    }
    printf $outfh "\n";
    #
    foreach my $machine (sort { $a <=> $b } keys %{$totals{$section}{$label}{by_machine_lane_stage_key1_key2}})
    {
        foreach my $lane (sort { $a <=> $b } keys %{$totals{$section}{$label}{by_machine_lane_stage_key1_key2}{$machine}})
        {
            foreach my $stage (sort { $a <=> $b } keys %{$totals{$section}{$label}{by_machine_lane_stage_key1_key2}{$machine}{$lane}})
            {
                foreach my $key1 (sort { $a <=> $b } keys %{$totals{$section}{$label}{by_machine_lane_stage_key1_key2}{$machine}{$lane}{$stage}})
                {
                    foreach my $key2 (sort { $a <=> $b } keys %{$totals{$section}{$label}{by_machine_lane_stage_key1_key2}{$machine}{$lane}{$stage}{$key1}})
                    {
                        printf $outfh "%s,%s,%s,%s,%s", $machine, $lane, $stage, $key1, $key2;
                        foreach my $col (@nozzle_count_cols)
                        {
                            printf $outfh ",%d", $totals{$section}{$label}{by_machine_lane_stage_key1_key2}{$machine}{$lane}{$stage}{$key1}{$key2}{$col};
                        }
                        printf $outfh "\n";
                    }
                }
            }
        }
    }
    close($outfh);
    #
    ###############################################################
    #
    $section = MOUNTPICKUPNOZZLE;
    #
    printf $log_fh "\n%d: Export Data For %s by Product, Machine, Lane, Stage, $nmkey1, $nmkey2, Blkserial:\n", __LINE__, $section;
    #
    $outfnm = "${export_dir}/NOZZLE_BY_PRODUCT_MACHINE_LANE_STAGE_${NMKEY1}_${NMKEY2}_BLKSERIAL.csv";
    printf $log_fh "%d: File %s already exists\n", __LINE__, $outfnm if ( -e $outfnm);
    open($outfh, ">" , $outfnm) || die $!;
    printf $outfh "product,";
    foreach my $pcol (@{$nozzle_export_cols_new{$label}})
    {
        printf $outfh $pcol->{format}, $pcol->{name};
    }
    printf $outfh "\n";
    #
    foreach my $product (sort keys %{$totals{$section}{by_product}})
    {
        foreach my $machine (sort { $a <=> $b } keys %{$totals{$section}{by_product}{$product}{$label}{by_machine_lane_stage_key1_key2_blkserial}})
        {
            foreach my $lane (sort { $a <=> $b } keys %{$totals{$section}{by_product}{$product}{$label}{by_machine_lane_stage_key1_key2_blkserial}{$machine}})
            {
                foreach my $stage (sort { $a <=> $b } keys %{$totals{$section}{by_product}{$product}{$label}{by_machine_lane_stage_key1_key2_blkserial}{$machine}{$lane}})
                {
                    foreach my $key1 (sort { $a <=> $b } keys %{$totals{$section}{by_product}{$product}{$label}{by_machine_lane_stage_key1_key2_blkserial}{$machine}{$lane}{$stage}})
                    {
                        foreach my $key2 (sort { $a <=> $b } keys %{$totals{$section}{by_product}{$product}{$label}{by_machine_lane_stage_key1_key2_blkserial}{$machine}{$lane}{$stage}{$key1}})
                        {
                            foreach my $blkserial (sort keys %{$totals{$section}{by_product}{$product}{$label}{by_machine_lane_stage_key1_key2_blkserial}{$machine}{$lane}{$stage}{$key1}{$key2}})
                            {
                                printf $outfh "%s,%s,%s,%s,%s,%s,%s", $product, $machine, $lane, $stage, $key1, $key2, $blkserial;
                                foreach my $col (@nozzle_count_cols)
                                {
                                    printf $outfh ",%d", $totals{$section}{by_product}{$product}{$label}{by_machine_lane_stage_key1_key2_blkserial}{$machine}{$lane}{$stage}{$key1}{$key2}{$blkserial}{$col};
                                }
                                printf $outfh "\n";
                            }
                        }
                    }
                }
            }
        }
    }
    close($outfh);
    #
    printf $log_fh "\n%d: Export Data For %s by Product, Machine, Lane, Stage, ${nmkey1}, ${nmkey2}:\n", __LINE__, $section;
    #
    $outfnm = "${export_dir}/NOZZLE_BY_PRODUCT_MACHINE_LANE_STAGE_${NMKEY1}_${NMKEY2}.csv";
    printf $log_fh "%d: File %s already exists\n", __LINE__, $outfnm if ( -e $outfnm);
    open($outfh, ">" , $outfnm) || die $!;
    printf $outfh "product,";
    foreach my $pcol (@{$nozzle_export_cols2_new{$label}})
    {
        printf $outfh $pcol->{format}, $pcol->{name};
    }
    printf $outfh "\n";
    #
    foreach my $product (sort keys %{$totals{$section}{by_product}})
    {
        foreach my $machine (sort { $a <=> $b } keys %{$totals{$section}{by_product}{$product}{$label}{by_machine_lane_stage_key1_key2}})
        {
            foreach my $lane (sort { $a <=> $b } keys %{$totals{$section}{by_product}{$product}{$label}{by_machine_lane_stage_key1_key2}{$machine}})
            {
                foreach my $stage (sort { $a <=> $b } keys %{$totals{$section}{by_product}{$product}{$label}{by_machine_lane_stage_key1_key2}{$machine}{$lane}})
                {
                    foreach my $key1 (sort { $a <=> $b } keys %{$totals{$section}{by_product}{$product}{$label}{by_machine_lane_stage_key1_key2}{$machine}{$lane}{$stage}})
                    {
                        foreach my $key2 (sort { $a <=> $b } keys %{$totals{$section}{by_product}{$product}{$label}{by_machine_lane_stage_key1_key2}{$machine}{$lane}{$stage}{$key1}})
                        {
                            printf $outfh "%s,%s,%s,%s,%s,%s",
                                $product, $machine, $lane, $stage, $key1, $key2;
                            foreach my $col (@nozzle_count_cols)
                            {
                                printf $outfh ",%d", $totals{$section}{by_product}{$product}{$label}{by_machine_lane_stage_key1_key2}{$machine}{$lane}{$stage}{$key1}{$key2}{$col};
                            }
                            printf $outfh "\n";
                        }
                    }
                }
            }
        }
    }
    close($outfh);
}
#
sub export_u01_nozzle_data_new
{
    my ($pdb) = @_;
    #
    export_u01_nozzle_data_keys($pdb, 
                                NZ_KEY_NHADD, 
                                NZ_KEY_NCADD, 
                                NZ_LABEL_NHADD_NCADD);
    export_u01_nozzle_data_keys($pdb, 
                                NZ_KEY_HEAD, 
                                NZ_KEY_NHADD, 
                                NZ_LABEL_HEAD_NHADD);
    export_u01_nozzle_data_keys($pdb, 
                                NZ_KEY_HEAD, 
                                NZ_KEY_NCADD, 
                                NZ_LABEL_HEAD_NCADD);
}
#
sub export_u01_data
{
    my ($pdb) = @_;
    #
    export_u01_count_data($pdb);
    export_u01_time_data($pdb);
    export_u01_feeder_data($pdb);
    if (($proc_options & PROC_OPT_USEOLDNZ) != 0)
    {
        export_u01_nozzle_data($pdb);
    }
    else
    {
        export_u01_nozzle_data_new($pdb);
    }
}
#
######################################################################
#
# high-level u01 file audit functions
#
sub calculate_u01_name_value_delta
{
    my ($pdb, $pu01, $section) = @_;
    #
    my $filename = $pu01->{file_name};
    #
    my $machine = $pu01->{mach_no};
    my $lane = $pu01->{lane};
    my $stage = $pu01->{stage};
    my $output_no = $pu01->{output_no};
    #
    foreach my $key (keys %{$pu01->{$section}->{data}})
    {
        my $delta = 0;
        #
        if (exists($pdb->{$section}->{$machine}{$lane}{$stage}{cache}{$key}))
        {
            $delta = 
                $pu01->{$section}->{data}->{$key} -
                $pdb->{$section}->{$machine}{$lane}{$stage}{cache}{$key};
            #
            if ($delta >= 0)
            {
                $pdb->{$section}->{$machine}{$lane}{$stage}{delta}{$key} = $delta;
            }
            elsif (($proc_options & PROC_OPT_USENEGDELTS) != 0)
            {
                printf $log_fh "%d: WARNING: [%s] using NEGATIVE delta for %s key %s: %d\n", __LINE__, $filename, $section, $key, $delta if ($verbose >= MINVERBOSE);
                $pdb->{$section}->{$machine}{$lane}{$stage}{delta}{$key} = $delta;
            }
            else
            {
                $pdb->{$section}->{$machine}{$lane}{$stage}{delta}{$key} = 0;
                printf $log_fh "%d: WARNING: [%s] setting NEGATIVE delta (%d) for %s key %s to ZERO\n", __LINE__, $filename, $delta, $section, $key if ($verbose >= MINVERBOSE);
            }
            #
            printf $log_fh "%d: %s: %s = %d\n", __LINE__, $section, $key, $delta if ($verbose >= MAXVERBOSE);
        }
        else
        {
            printf $log_fh "%d: ERROR: [%s] %s key %s NOT found in cache. Ignoring counts (%d).\n", __LINE__, $filename, $section, $key, $pu01->{$section}->{data}->{$key};
            die "ERROR: [$filename] $section key $key NOT found it cache. Stopped";
        }
    }
}
#
sub copy_u01_name_value_cache
{
    my ($pdb, $pu01, $section) = @_;
    #
    my $filename = $pu01->{file_name};
    #
    my $machine = $pu01->{mach_no};
    my $lane = $pu01->{lane};
    my $stage = $pu01->{stage};
    my $output_no = $pu01->{output_no};
    #
    foreach my $key (keys %{$pu01->{$section}->{data}})
    {
        $pdb->{$section}->{$machine}{$lane}{$stage}{cache}{$key} =
            $pu01->{$section}->{data}->{$key};
    }
}
#
sub copy_u01_name_value_delta
{
    my ($pdb, $pu01, $section) = @_;
    #
    my $filename = $pu01->{file_name};
    #
    my $machine = $pu01->{mach_no};
    my $lane = $pu01->{lane};
    my $stage = $pu01->{stage};
    my $output_no = $pu01->{output_no};
    #
    delete $pdb->{$section}->{$machine}{$lane}{$stage}{delta};
    #
    foreach my $key (keys %{$pu01->{$section}->{data}})
    {
        my $delta = $pu01->{$section}->{data}->{$key};
        $pdb->{$section}->{$machine}{$lane}{$stage}{delta}{$key} = $delta;
        printf $log_fh "%d: %s: %s = %d\n", __LINE__, $section, $key, $delta
            if ($verbose >= MAXVERBOSE);
    }
}
#
sub tabulate_u01_name_value_delta
{
    my ($pdb, $pu01, $section) = @_;
    #
    my $filename = $pu01->{file_name};
    #
    my $machine = $pu01->{mach_no};
    my $lane = $pu01->{lane};
    my $stage = $pu01->{stage};
    my $output_no = $pu01->{output_no};
    #
    my $product = $pdb->{product}{u01}{$machine}{$lane}{$stage};
    #
    foreach my $key (keys %{$pu01->{$section}->{data}})
    {
        #
        # product dependent totals
        #
        if (exists($totals{$section}{by_product}{$product}{totals}{$key}))
        {
            $totals{$section}{by_product}{$product}{totals}{$key} += 
                $pdb->{$section}->{$machine}{$lane}{$stage}{delta}{$key};
        }
        else
        {
            $totals{$section}{by_product}{$product}{totals}{$key} = 
                $pdb->{$section}->{$machine}{$lane}{$stage}{delta}{$key};
        }
        printf $log_fh "%d: %s %s %s total=%d\n", __LINE__, $product, $section, $key, $totals{$section}{by_product}{$product}{totals}{$key} if ($verbose >= MAXVERBOSE);
        #
        if (exists($totals{$section}{by_product}{$product}{by_machine}{$machine}{$key}))
        {
            $totals{$section}{by_product}{$product}{by_machine}{$machine}{$key} += 
                $pdb->{$section}->{$machine}{$lane}{$stage}{delta}{$key};
        }
        else
        {
            $totals{$section}{by_product}{$product}{by_machine}{$machine}{$key} = 
                $pdb->{$section}->{$machine}{$lane}{$stage}{delta}{$key};
        }
        printf $log_fh "%d: %s %s %s %s total=%d\n", __LINE__, $product, $section, $machine, $key, $totals{$section}{by_product}{$product}{by_machine}{$machine}{$key} if ($verbose >= MAXVERBOSE);
        #
        if (exists($totals{$section}{by_product}{$product}{by_machine_lane}{$machine}{$lane}{$key}))
        {
            $totals{$section}{by_product}{$product}{by_machine_lane}{$machine}{$lane}{$key} += 
                $pdb->{$section}->{$machine}{$lane}{$stage}{delta}{$key};
        }
        else
        {
            $totals{$section}{by_product}{$product}{by_machine_lane}{$machine}{$lane}{$key} = 
                $pdb->{$section}->{$machine}{$lane}{$stage}{delta}{$key};
        }
        printf $log_fh "%d: %s %s %s %s %s total=%d\n", __LINE__, $product, $section, $machine, $lane, $key, $totals{$section}{by_product}{$product}{by_machine_lane}{$machine}{$lane}{$key} if ($verbose >= MAXVERBOSE);
        #
        if (exists($totals{$section}{by_product}{$product}{by_machine_lane_stage}{$machine}{$lane}{$stage}{$key}))
        {
            $totals{$section}{by_product}{$product}{by_machine_lane_stage}{$machine}{$lane}{$stage}{$key} += 
                $pdb->{$section}->{$machine}{$lane}{$stage}{delta}{$key};
        }
        else
        {
            $totals{$section}{by_product}{$product}{by_machine_lane_stage}{$machine}{$lane}{$stage}{$key} = 
                $pdb->{$section}->{$machine}{$lane}{$stage}{delta}{$key};
        }
        printf $log_fh "%d: %s %s %s %s %s %s total=%d\n", __LINE__, $product, $section, $machine, $lane, $stage, $key, $totals{$section}{by_product}{$product}{by_machine_lane_stage}{$machine}{$lane}{$stage}{$key} if ($verbose >= MAXVERBOSE);
        #
        # product independent totals
        #
        if (exists($totals{$section}{totals}{$key}))
        {
            $totals{$section}{totals}{$key} += 
                $pdb->{$section}->{$machine}{$lane}{$stage}{delta}{$key};
        }
        else
        {
            $totals{$section}{totals}{$key} = 
                $pdb->{$section}->{$machine}{$lane}{$stage}{delta}{$key};
        }
        #
        if (exists($totals{$section}{by_machine}{$machine}{$key}))
        {
            $totals{$section}{by_machine}{$machine}{$key} += 
                $pdb->{$section}->{$machine}{$lane}{$stage}{delta}{$key};
        }
        else
        {
            $totals{$section}{by_machine}{$machine}{$key} = 
                $pdb->{$section}->{$machine}{$lane}{$stage}{delta}{$key};
        }
        #
        if (exists($totals{$section}{by_machine_lane}{$machine}{$lane}{$key}))
        {
            $totals{$section}{by_machine_lane}{$machine}{$lane}{$key} += 
                $pdb->{$section}->{$machine}{$lane}{$stage}{delta}{$key};
        }
        else
        {
            $totals{$section}{by_machine_lane}{$machine}{$lane}{$key} = 
                $pdb->{$section}->{$machine}{$lane}{$stage}{delta}{$key};
        }
        #
        if (exists($totals{$section}{by_machine_lane_stage}{$machine}{$lane}{$stage}{$key}))
        {
            $totals{$section}{by_machine_lane_stage}{$machine}{$lane}{$stage}{$key} += 
                $pdb->{$section}->{$machine}{$lane}{$stage}{delta}{$key};
        }
        else
        {
            $totals{$section}{by_machine_lane_stage}{$machine}{$lane}{$stage}{$key} = 
                $pdb->{$section}->{$machine}{$lane}{$stage}{delta}{$key};
        }
    }
}
#
sub audit_u01_name_value
{
    my ($pdb, $pu01, $section) = @_;
    #
    my $filename = $pu01->{file_name};
    my $machine = $pu01->{mach_no};
    my $lane = $pu01->{lane};
    my $stage = $pu01->{stage};
    my $output_no = $pu01->{output_no};
    #
    return if ((($proc_options & PROC_OPT_IGNRESET12) != 0) &&
               ($output_no == TIMER_NOT_RUNNING) &&
               ($section eq COUNT));
    #
    my $mjsid = '';
    my $lotname = '';
    my $lotnumber = 0;
    #
    my $change_over = $pdb->{change_over}{u01}{$machine}{$lane}{$stage};
    printf $log_fh "%d: Change Over: %s\n", __LINE__, $change_over if ($verbose >= MAXVERBOSE);
    #
    get_product_info($pu01, \$mjsid, \$lotname, \$lotnumber);
    #
    printf $log_fh "\n%d: SECTION  : %s\n", __LINE__, $section
        if ($verbose >= MAXVERBOSE);
    #
    if ($verbose >= MAXVERBOSE)
    {
        printf $log_fh "%d: MACHINE  : %s\n", __LINE__, $machine;
        printf $log_fh "%d: LANE     : %d\n", __LINE__, $lane;
        printf $log_fh "%d: STAGE    : %d\n", __LINE__, $stage;
        printf $log_fh "%d: OUTPUT NO: %s\n", __LINE__, $output_no;
        printf $log_fh "%d: FILE RECS : %d\n", __LINE__, scalar(@{$pu01->{data}});
        printf $log_fh "%d: %s RECS: %d\n", __LINE__, $section, scalar(keys %{$pu01->{$section}->{data}});
    }
    #
    # output 3,4,5,12 U01 files have both Time and Count sections.
    # these output types can all be treated the same.
    #
    if (($output_no == PROD_COMPLETE) ||
        ($output_no == PROD_COMPLETE_LATER) ||
        ($output_no == DETECT_CHANGE) ||
        ($output_no == TIMER_NOT_RUNNING))
    {
        if ( ! exists($pdb->{$section}->{$machine}{$lane}{$stage}{state}))
        {
            #
            # first file of any of these types to be processed.
            #
            printf $log_fh "%d: ENTRY STATE: UNKNOWN\n", __LINE__
                if ($verbose >= MAXVERBOSE);
            #
            delete $pdb->{$section}->{$machine}{$lane}{$stage}{cache};
            copy_u01_name_value_cache($pdb, $pu01, $section);
            #
            $pdb->{$section}->{$machine}{$lane}{$stage}{state} = DELTA;
        }
        else
        {
            my $state = $pdb->{$section}->{$machine}{$lane}{$stage}{state};
            #
            printf $log_fh "%d: ENTRY STATE: %s\n", __LINE__, $state if ($verbose >= MAXVERBOSE);
            #
            if ($change_over == TRUE)
            {
                copy_u01_name_value_delta($pdb, $pu01, $section);
                tabulate_u01_name_value_delta($pdb, $pu01, $section);
                copy_u01_name_value_cache($pdb, $pu01, $section);
                #
                $pdb->{$section}->{$machine}{$lane}{$stage}{state} = DELTA;
            }
            elsif ($state eq DELTA)
            {
                calculate_u01_name_value_delta($pdb, $pu01, $section);
                tabulate_u01_name_value_delta($pdb, $pu01, $section);
                copy_u01_name_value_cache($pdb, $pu01, $section);
                #
                $pdb->{$section}->{$machine}{$lane}{$stage}{state} = DELTA;
            }
            elsif ($state eq RESET)
            {
                copy_u01_name_value_delta($pdb, $pu01, $section);
                tabulate_u01_name_value_delta($pdb, $pu01, $section);
                copy_u01_name_value_cache($pdb, $pu01, $section);
                #
                $pdb->{$section}->{$machine}{$lane}{$stage}{state} = DELTA;
            }
            elsif ($state eq BASELINE)
            {
                delete $pdb->{$section}->{$machine}{$lane}{$stage}{cache};
                copy_u01_name_value_cache($pdb, $pu01, $section);
                #
                $pdb->{$section}->{$machine}{$lane}{$stage}{state} = DELTA;
            }
            else
            {
                die "ERROR: unknown $section state: $state. Stopped";
            }
            printf $log_fh "%d: EXIT STATE: %s\n", __LINE__, $state if ($verbose >= MAXVERBOSE);
        }
    }
    elsif (($output_no == MANUAL_CLEAR) ||
           ($output_no == AUTO_CLEAR))
    {
        #
        # reset files have no data. they indicate the machine 
        # and counters were all reset to zero.
        #
        my $state = $pdb->{$section}->{$machine}{$lane}{$stage}{state};
        printf $log_fh "%d: ENTRY STATE: %s\n", __LINE__, $state if ($verbose >= MAXVERBOSE);
        #
        delete $pdb->{$section}->{$machine}{$lane}{$stage}{cache};
        #
        $pdb->{$section}->{$machine}{$lane}{$stage}{state} = RESET;
        printf $log_fh "%d: EXIT STATE: %s\n", __LINE__, $state if ($verbose >= MAXVERBOSE);
    }
    else
    {
        die "ERROR: unknown output type: $output_no. Stopped";
    }
    #
    return;
}
#
######################################################################
#
# routines for feeder section
# 
sub calculate_u01_feeder_delta
{
    my ($pdb, $pu01) = @_;
    #
    my $section = MOUNTPICKUPFEEDER;
    #
    my $filename = $pu01->{file_name};
    my $machine = $pu01->{mach_no};
    my $lane = $pu01->{lane};
    my $stage = $pu01->{stage};
    my $output_no = $pu01->{output_no};
    #
    my $pcols = $pu01->{$section}->{column_names};
    #
    delete $pdb->{$section}->{$machine}{$lane}{$stage}{delta};
    #
    foreach my $prow (@{$pu01->{$section}->{data}})
    {
        my $fadd = $prow->{FAdd};
        my $fsadd = $prow->{FSAdd};
        my $reelid = $prow->{ReelID};
        #
        my $is_tray = substr($fadd, -4, 2);
        if ($is_tray > 0)
        {
            $is_tray = TRUE;
            printf $log_fh "%d: [%s] %s IS tray part (%s) fadd: %s, fsadd: %s\n", __LINE__, $filename, $section, $is_tray, $fadd, $fsadd
                if ($verbose >= MAXVERBOSE);
        }
        else
        {
            $is_tray = FALSE;
            printf $log_fh "%d: [%s] %s IS NOT tray part (%s) fadd: %s, fsadd: %s\n", __LINE__, $filename, $section, $is_tray, $fadd, $fsadd
                if ($verbose >= MAXVERBOSE);
        }
        #
        if ( ! exists($pdb->{$section}->{$machine}{$lane}{$stage}{cache}{$fadd}{$fsadd}{data}))
        {
            #
            # unlike name-value (count,time) sections, it is possible
            # to get new entries which have not been seen before. for
            # example, new reelids or new feeders may not be in the
            # previous u01 file, but appear as new. in those cases,
            # take the counts as is.
            #
            printf $log_fh "%d: WARNING: [%s] %s FAdd %s, FSAdd %s NOT found in cache. Taking all counts as is.\n", __LINE__, $filename, $section, $fadd, $fsadd if ($verbose >= MINVERBOSE);
            foreach my $col (@{$pcols})
            {
                $pdb->{$section}->{$machine}{$lane}{$stage}{delta}{$fadd}{$fsadd}{data}{$col} = $prow->{$col};
            }
        }
        else
        {
            my $cache_reelid = $pdb->{$section}->{$machine}{$lane}{$stage}{cache}{$fadd}{$fsadd}{data}{ReelID};
            my $cache_filename = $pdb->{$section}->{$machine}{$lane}{$stage}{cache}{$fadd}{$fsadd}{filename};
            if (($reelid eq $cache_reelid) || ($is_tray == TRUE))
            {
                $pdb->{$section}->{$machine}{$lane}{$stage}{delta}{$fadd}{$fsadd}{data}{ReelID} = $reelid;
                #
                foreach my $col (@feeder_count_cols)
                {
                    my $u01_value = $prow->{$col};
                    my $cache_value = $pdb->{$section}->{$machine}{$lane}{$stage}{cache}{$fadd}{$fsadd}{data}{$col};
                    #
                    my $delta = $u01_value - $cache_value;
                    #
                    if ($delta >= 0)
                    {
                        $pdb->{$section}->{$machine}{$lane}{$stage}{delta}{$fadd}{$fsadd}{data}{$col} = $delta;
                    }
                    elsif (($proc_options & PROC_OPT_USENEGDELTS) != 0)
                    {
                         printf $log_fh "%d: WARNING: [%s] [%s] %s FAdd %s, FSAdd %s using NEGATIVE delta for key %s: %d\n", __LINE__, $filename, $cache_filename, $section, $fadd, $fsadd, $col, $delta if ($verbose >= MINVERBOSE);
                        $pdb->{$section}->{$machine}{$lane}{$stage}{delta}{$fadd}{$fsadd}{data}{$col} = $delta;
                    }
                    else
                    {
                        $pdb->{$section}->{$machine}{$lane}{$stage}{delta}{$fadd}{$fsadd}{data}{$col} = 0;
                         printf $log_fh "%d: WARNING: [%s] [%s] %s FAdd %s, FSAdd %s setting NEGATIVE delta (%d) for key %s to ZERO; current value %d, cache value %d\n", __LINE__, $filename, $cache_filename, $section, $fadd, $fsadd, $delta, $col, $u01_value, $cache_value if ($verbose >= MINVERBOSE);
                    }
                }
            }
            else
            {
                printf $log_fh "%d: WARNING: [%s] %s FAdd %s, FSAdd %s REELID CHANGED: CACHED %s, CURRENT U01 %s\n", __LINE__, $filename, $section, $fadd, $fsadd, $cache_reelid, $reelid if ($verbose >= MINVERBOSE);
                #
                delete $pdb->{$section}->{$machine}{$lane}{$stage}{delta}{$fadd}{$fsadd}{data};
                #
                foreach my $col (@{$pcols})
                {
                    $pdb->{$section}->{$machine}{$lane}{$stage}{delta}{$fadd}{$fsadd}{data}{$col} = $prow->{$col};
                }
            }
        }
    }
}
#
sub copy_u01_feeder_cache
{
    my ($pdb, $pu01, $state) = @_;
    #
    my $section = MOUNTPICKUPFEEDER;
    #
    my $filename = $pu01->{file_name};
    my $machine = $pu01->{mach_no};
    my $lane = $pu01->{lane};
    my $stage = $pu01->{stage};
    my $output_no = $pu01->{output_no};
    #
    my $pcols = $pu01->{$section}->{column_names};
    #
    foreach my $prow (@{$pu01->{$section}->{data}})
    {
        my $fadd = $prow->{FAdd};
        my $fsadd = $prow->{FSAdd};
        #
        foreach my $col (@{$pcols})
        {
            $pdb->{$section}->{$machine}{$lane}{$stage}{cache}{$fadd}{$fsadd}{data}{$col} = $prow->{$col};
        }
        #
        $pdb->{$section}->{$machine}{$lane}{$stage}{cache}{$fadd}{$fsadd}{state} = $state;
        $pdb->{$section}->{$machine}{$lane}{$stage}{cache}{$fadd}{$fsadd}{filename} = $filename;
    }
}
#
sub copy_u01_feeder_delta
{
    my ($pdb, $pu01) = @_;
    #
    my $section = MOUNTPICKUPFEEDER;
    #
    my $filename = $pu01->{file_name};
    my $machine = $pu01->{mach_no};
    my $lane = $pu01->{lane};
    my $stage = $pu01->{stage};
    my $output_no = $pu01->{output_no};
    #
    my $pcols = $pu01->{$section}->{column_names};
    #
    delete $pdb->{$section}->{$machine}{$lane}{$stage}{delta};
    #
    foreach my $prow (@{$pu01->{$section}->{data}})
    {
        my $fadd = $prow->{FAdd};
        my $fsadd = $prow->{FSAdd};
        #
        foreach my $col (@{$pcols})
        {
            $pdb->{$section}->{$machine}{$lane}{$stage}{delta}{$fadd}{$fsadd}{data}{$col} = $prow->{$col};
        }
    }
}
#
sub tabulate_u01_feeder_delta
{
    my ($pdb, $pu01) = @_;
    #
    my $filename = $pu01->{file_name};
    #
    my $machine = $pu01->{mach_no};
    my $lane = $pu01->{lane};
    my $stage = $pu01->{stage};
    my $output_no = $pu01->{output_no};
    my $section = MOUNTPICKUPFEEDER;
    #
    my $product = $pdb->{product}{u01}{$machine}{$lane}{$stage};
    #
    foreach my $fadd (sort { $a <=> $b } keys %{$pdb->{$section}{$machine}{$lane}{$stage}{delta}})
    {
        my $table_no = int($fadd/10000); # truncate
        #
        foreach my $fsadd (sort { $a <=> $b } keys %{$pdb->{$section}{$machine}{$lane}{$stage}{delta}{$fadd}})
        {
            my $reelid = $pdb->{$section}{$machine}{$lane}{$stage}{delta}{$fadd}{$fsadd}{data}{ReelID};
            #
            # product-independent totals
            #
            #  by_machine_lane_stage_fadd_fsadd_reelid
            #
            if (exists($totals{$section}{by_machine_lane_stage_fadd_fsadd_reelid}{$machine}{$lane}{$stage}{$fadd}{$fsadd}{$reelid}))
            {
                foreach my $col (@feeder_count_cols)
                {
                    $totals{$section}{by_machine_lane_stage_fadd_fsadd_reelid}{$machine}{$lane}{$stage}{$fadd}{$fsadd}{$reelid}{$col} += $pdb->{$section}{$machine}{$lane}{$stage}{delta}{$fadd}{$fsadd}{data}{$col};
                }
            }
            else
            {
                foreach my $col (@feeder_count_cols)
                {
                    $totals{$section}{by_machine_lane_stage_fadd_fsadd_reelid}{$machine}{$lane}{$stage}{$fadd}{$fsadd}{$reelid}{$col} = $pdb->{$section}{$machine}{$lane}{$stage}{delta}{$fadd}{$fsadd}{data}{$col};
                }
            }
            #
            # by_machine_lane_stage_fadd_fsadd
            #
            if (exists($totals{$section}{by_machine_lane_stage_fadd_fsadd}{$machine}{$lane}{$stage}{$fadd}{$fsadd}))
            {
                foreach my $col (@feeder_count_cols)
                {
                    $totals{$section}{by_machine_lane_stage_fadd_fsadd}{$machine}{$lane}{$stage}{$fadd}{$fsadd}{$col} += $pdb->{$section}{$machine}{$lane}{$stage}{delta}{$fadd}{$fsadd}{data}{$col};
                }
            }
            else
            {
                foreach my $col (@feeder_count_cols)
                {
                    $totals{$section}{by_machine_lane_stage_fadd_fsadd}{$machine}{$lane}{$stage}{$fadd}{$fsadd}{$col} = $pdb->{$section}{$machine}{$lane}{$stage}{delta}{$fadd}{$fsadd}{data}{$col};
                }
            }
            #
            # by_machine_lane_stage_table_no
            #
            if (exists($totals{$section}{by_machine_lane_stage_table_no}{$machine}{$lane}{$stage}{$table_no}))
            {
                foreach my $col (@feeder_count_cols)
                {
                    $totals{$section}{by_machine_lane_stage_table_no}{$machine}{$lane}{$stage}{$table_no}{$col} += $pdb->{$section}{$machine}{$lane}{$stage}{delta}{$fadd}{$fsadd}{data}{$col};
                }
            }
            else
            {
                foreach my $col (@feeder_count_cols)
                {
                    $totals{$section}{by_machine_lane_stage_table_no}{$machine}{$lane}{$stage}{$table_no}{$col} = $pdb->{$section}{$machine}{$lane}{$stage}{delta}{$fadd}{$fsadd}{data}{$col};
                }
            }
            #
            # product-dependent totals
            #
            # by_product by_machine_lane_stage_fadd_fsadd_reelid
            #
            if (exists($totals{$section}{by_product}{$product}{by_machine_lane_stage_fadd_fsadd_reelid}{$machine}{$lane}{$stage}{$fadd}{$fsadd}{$reelid}))
            {
                foreach my $col (@feeder_count_cols)
                {
                    $totals{$section}{by_product}{$product}{by_machine_lane_stage_fadd_fsadd_reelid}{$machine}{$lane}{$stage}{$fadd}{$fsadd}{$reelid}{$col} += $pdb->{$section}{$machine}{$lane}{$stage}{delta}{$fadd}{$fsadd}{data}{$col};
                }
            }
            else
            {
                foreach my $col (@feeder_count_cols)
                {
                    $totals{$section}{by_product}{$product}{by_machine_lane_stage_fadd_fsadd_reelid}{$machine}{$lane}{$stage}{$fadd}{$fsadd}{$reelid}{$col} = $pdb->{$section}{$machine}{$lane}{$stage}{delta}{$fadd}{$fsadd}{data}{$col};
                }
            }
            #
            # by_product by_machine_lane_stage_fadd_fsadd
            #
            if (exists($totals{$section}{by_product}{$product}{by_machine_lane_stage_fadd_fsadd}{$machine}{$lane}{$stage}{$fadd}{$fsadd}))
            {
                foreach my $col (@feeder_count_cols)
                {
                    $totals{$section}{by_product}{$product}{by_machine_lane_stage_fadd_fsadd}{$machine}{$lane}{$stage}{$fadd}{$fsadd}{$col} += $pdb->{$section}{$machine}{$lane}{$stage}{delta}{$fadd}{$fsadd}{data}{$col};
                }
            }
            else
            {
                foreach my $col (@feeder_count_cols)
                {
                    $totals{$section}{by_product}{$product}{by_machine_lane_stage_fadd_fsadd}{$machine}{$lane}{$stage}{$fadd}{$fsadd}{$col} = $pdb->{$section}{$machine}{$lane}{$stage}{delta}{$fadd}{$fsadd}{data}{$col};
                }
            }
            #
            # by_product by_machine_lane_stage_table_no
            #
            if (exists($totals{$section}{by_product}{$product}{by_machine_lane_stage_table_no}{$machine}{$lane}{$stage}{$table_no}))
            {
                foreach my $col (@feeder_count_cols)
                {
                    $totals{$section}{by_product}{$product}{by_machine_lane_stage_table_no}{$machine}{$lane}{$stage}{$table_no}{$col} += $pdb->{$section}{$machine}{$lane}{$stage}{delta}{$fadd}{$fsadd}{data}{$col};
                }
            }
            else
            {
                foreach my $col (@feeder_count_cols)
                {
                    $totals{$section}{by_product}{$product}{by_machine_lane_stage_table_no}{$machine}{$lane}{$stage}{$table_no}{$col} = $pdb->{$section}{$machine}{$lane}{$stage}{delta}{$fadd}{$fsadd}{data}{$col};
                }
            }
        }
    }
}
#
sub audit_u01_feeders
{
    my ($pdb, $pu01) = @_;
    #
    my $machine = $pu01->{mach_no};
    my $lane = $pu01->{lane};
    my $stage = $pu01->{stage};
    my $output_no = $pu01->{output_no};
    my $section = MOUNTPICKUPFEEDER;
    my $filename = $pu01->{file_name};
    #
    set_list_section_column_names(LNB_U01_FILE_TYPE, $pu01, $section);
    #
    printf $log_fh "\n%d: SECTION  : %s\n", __LINE__, $section
        if ($verbose >= MAXVERBOSE);
    #
    if ($verbose >= MAXVERBOSE)
    {
        printf $log_fh "%d: MACHINE  : %s\n", __LINE__, $machine;
        printf $log_fh "%d: LANE     : %d\n", __LINE__, $lane;
        printf $log_fh "%d: STAGE    : %d\n", __LINE__, $stage;
        printf $log_fh "%d: OUTPUT NO: %s\n", __LINE__, $output_no;
        printf $log_fh "%d: FILE RECS : %d\n", __LINE__, scalar(@{$pu01->{data}});
        printf $log_fh "%d: %s RECS: %d\n", __LINE__, $section, scalar(@{$pu01->{$section}->{data}}) if (defined(@{$pu01->{$section}->{data}}));
    }
    #
    # check if the file has a feeder data section.
    #
    if ($output_no == TIMER_NOT_RUNNING)
    {
        printf $log_fh "%d: No Feeder data in Output=%d U01 files. Skipping.\n", __LINE__, $output_no if ($verbose >= MAXVERBOSE);
        return;
    }
    elsif (($output_no == PROD_COMPLETE) ||
           ($output_no == PROD_COMPLETE_LATER))
    {
        if ( ! exists($pdb->{$section}->{$machine}{$lane}{$stage}{state}))
        {
            printf $log_fh "%d: ENTRY STATE: UNKNOWN\n", __LINE__
                if ($verbose >= MAXVERBOSE);
            #
            delete $pdb->{$section}->{$machine}{$lane}{$stage}{cache};
            copy_u01_feeder_cache($pdb, $pu01, DELTA);
            #
            $pdb->{$section}->{$machine}{$lane}{$stage}{state} = DELTA;
        }
        elsif ($pdb->{$section}->{$machine}{$lane}{$stage}{state} eq RESET)
        {
            printf $log_fh "%d: ENTRY STATE: %s\n", __LINE__,
                $pdb->{$section}->{$machine}{$lane}{$stage}{state}
                if ($verbose >= MAXVERBOSE);
            copy_u01_feeder_delta($pdb, $pu01);
            tabulate_u01_feeder_delta($pdb, $pu01);
            copy_u01_feeder_cache($pdb, $pu01, DELTA);
            #
            $pdb->{$section}->{$machine}{$lane}{$stage}{state} = DELTA;
        }
        elsif ($pdb->{$section}->{$machine}{$lane}{$stage}{state} eq DELTA)
        {
            printf $log_fh "%d: ENTRY STATE: %s\n", __LINE__,
                $pdb->{$section}->{$machine}{$lane}{$stage}{state}
                if ($verbose >= MAXVERBOSE);
            calculate_u01_feeder_delta($pdb, $pu01);
            tabulate_u01_feeder_delta($pdb, $pu01);
            copy_u01_feeder_cache($pdb, $pu01, DELTA);
            #
            $pdb->{$section}->{$machine}{$lane}{$stage}{state} = DELTA;
        }
        elsif ($pdb->{$section}->{$machine}{$lane}{$stage}{state} eq BASELINE)
        {
            printf $log_fh "%d: ENTRY STATE: %s\n", __LINE__,
                $pdb->{$section}->{$machine}{$lane}{$stage}{state}
                if ($verbose >= MAXVERBOSE);
            delete $pdb->{$section}->{$machine}{$lane}{$stage}{cache};
            #
            copy_u01_feeder_cache($pdb, $pu01, DELTA);
            #
            $pdb->{$section}->{$machine}{$lane}{$stage}{state} = DELTA;
        }
        else
        {
            my $state = $pdb->{$section}->{$machine}{$lane}{$stage}{state};
            die "ERROR: unknown $section state: $state. Stopped";
        }
    }
    elsif ($output_no == DETECT_CHANGE)
    {
        if ( ! exists($pdb->{$section}->{$machine}{$lane}{$stage}{state}))
        {
            printf $log_fh "%d: ENTRY STATE: UNKNOWN\n", __LINE__,
                if ($verbose >= MAXVERBOSE);
            #
            copy_u01_feeder_cache($pdb, $pu01, DELTA);
            $pdb->{$section}->{$machine}{$lane}{$stage}{state} = BASELINE;
        }
        elsif ($pdb->{$section}->{$machine}{$lane}{$stage}{state} eq RESET)
        {
            printf $log_fh "%d: ENTRY STATE: %s\n", __LINE__,
                $pdb->{$section}->{$machine}{$lane}{$stage}{state}
                if ($verbose >= MAXVERBOSE);
            copy_u01_feeder_delta($pdb, $pu01);
            tabulate_u01_feeder_delta($pdb, $pu01);
            copy_u01_feeder_cache($pdb, $pu01, DELTA);
            #
            $pdb->{$section}->{$machine}{$lane}{$stage}{state} = DELTA;
        }
        elsif ($pdb->{$section}->{$machine}{$lane}{$stage}{state} eq DELTA)
        {
            printf $log_fh "%d: ENTRY STATE: %s\n", __LINE__,__LINE__,
                $pdb->{$section}->{$machine}{$lane}{$stage}{state}
                if ($verbose >= MAXVERBOSE);
            calculate_u01_feeder_delta($pdb, $pu01);
            tabulate_u01_feeder_delta($pdb, $pu01);
            copy_u01_feeder_cache($pdb, $pu01, DELTA);
            #
            $pdb->{$section}->{$machine}{$lane}{$stage}{state} = DELTA;
        }
        elsif ($pdb->{$section}->{$machine}{$lane}{$stage}{state} eq BASELINE)
        {
            printf $log_fh "%d: ENTRY STATE: %s\n", __LINE__,
                $pdb->{$section}->{$machine}{$lane}{$stage}{state}
                if ($verbose >= MAXVERBOSE);
            #
            copy_u01_feeder_cache($pdb, $pu01, DELTA);
            $pdb->{$section}->{$machine}{$lane}{$stage}{state} = DELTA;
        }
        else
        {
            my $state = $pdb->{$section}->{$machine}{$lane}{$stage}{state};
            die "ERROR: unknown $section state: $state. Stopped";
        }
    }
    elsif (($output_no == MANUAL_CLEAR) ||
           ($output_no == AUTO_CLEAR))
    {
        printf $log_fh "%D: ENTRY STATE: %s\n", __LINE__,
            $pdb->{$section}->{$machine}{$lane}{$stage}{state}
            if ($verbose >= MAXVERBOSE);
        $pdb->{$section}->{$machine}{$lane}{$stage}{state} = RESET;
        delete $pdb->{$section}->{$machine}{$lane}{$stage}{cache};
    }
    else
    {
        die "ERROR: unknown $section output type: $output_no. Stopped";
    }
    #
    printf $log_fh "%d: EXIT STATE: %s\n", __LINE__,
        $pdb->{$section}->{$machine}{$lane}{$stage}{state}
        if ($verbose >= MAXVERBOSE);
    #
    return;
}
#
######################################################################
#
# routines for nozzle section
#
sub calculate_u01_nozzle_delta
{
    my ($pdb, $pu01) = @_;
    #
    my $section = MOUNTPICKUPNOZZLE;
    #
    my $filename = $pu01->{file_name};
    my $machine = $pu01->{mach_no};
    my $lane = $pu01->{lane};
    my $stage = $pu01->{stage};
    my $output_no = $pu01->{output_no};
    #
    my $pcols = $pu01->{$section}->{column_names};
    #
    delete $pdb->{$section}->{$machine}{$lane}{$stage}{delta};
    #
    foreach my $prow (@{$pu01->{$section}->{data}})
    {
        my $nhadd = $prow->{NHAdd};
        my $ncadd = $prow->{NCAdd};
        my $blkserial = $prow->{BLKSerial};
        #
        if ( ! exists($pdb->{$section}->{$machine}{$lane}{$stage}{cache}{$nhadd}{$ncadd}{data}))
        {
            printf $log_fh "%d: WARNING: [%s] %s NHAdd %s, NCAdd %s NOT found in cache. Taking all counts as is.\n", __LINE__, $filename, $section, $nhadd, $ncadd if ($verbose >= MINVERBOSE);
            foreach my $col (@{$pcols})
            {
                $pdb->{$section}->{$machine}{$lane}{$stage}{delta}{$nhadd}{$ncadd}{data}{$col} = $prow->{$col};
            }
        }
        else
        {
            my $cache_blkserial = $pdb->{$section}->{$machine}{$lane}{$stage}{cache}{$nhadd}{$ncadd}{data}{BLKSerial};
            if ($blkserial eq $cache_blkserial)
            {
                $pdb->{$section}->{$machine}{$lane}{$stage}{delta}{$nhadd}{$ncadd}{data}{BLKSerial} = $blkserial;
                #
                foreach my $col (@nozzle_count_cols)
                {
                    my $u01_value = $prow->{$col};
                    my $cache_value = $pdb->{$section}->{$machine}{$lane}{$stage}{cache}{$nhadd}{$ncadd}{data}{$col};
                    #
                    my $delta = $u01_value - $cache_value;
                    #
                    if ($delta >= 0)
                    {
                        $pdb->{$section}->{$machine}{$lane}{$stage}{delta}{$nhadd}{$ncadd}{data}{$col} = $delta;
                    }
                    elsif (($proc_options & PROC_OPT_USENEGDELTS) != 0)
                    {
                         printf $log_fh "%d: WARNING: [%s] %s NHAdd %s, NCAdd %s using NEGATIVE delta for key %s: %d\n", __LINE__, $filename, $section, $nhadd, $ncadd, $col, $delta if ($verbose >= MINVERBOSE);
                        $pdb->{$section}->{$machine}{$lane}{$stage}{delta}{$nhadd}{$ncadd}{data}{$col} = $delta;
                    }
                    else
                    {
                        $pdb->{$section}->{$machine}{$lane}{$stage}{delta}{$nhadd}{$ncadd}{data}{$col} = 0;
                         printf $log_fh "%d: WARNING: [%s] %s NHAdd %s, NCAdd %s setting NEGATIVE delta (%d) for key %s to ZERO\n", __LINE__, $filename, $section, $nhadd, $ncadd, $delta, $col if ($verbose >= MINVERBOSE);
                    }
                }
            }
            else
            {
                printf $log_fh "%d: WARNING: [%s] %s NHAdd %s, NCAdd %s BLKSERIAL CHANGED: CACHED %s, CURRENT U01 %s\n", __LINE__, $filename, $section, $nhadd, $ncadd, $cache_blkserial, $blkserial if ($verbose >= MINVERBOSE);
                #
                delete $pdb->{$section}->{$machine}{$lane}{$stage}{delta}{$nhadd}{$ncadd}{data};
                #
                foreach my $col (@{$pcols})
                {
                    $pdb->{$section}->{$machine}{$lane}{$stage}{delta}{$nhadd}{$ncadd}{data}{$col} = $prow->{$col};
                }
            }
        }
    }
}
#
sub copy_u01_nozzle_cache
{
    my ($pdb, $pu01, $state) = @_;
    #
    my $section = MOUNTPICKUPNOZZLE;
    #
    my $filename = $pu01->{file_name};
    my $machine = $pu01->{mach_no};
    my $lane = $pu01->{lane};
    my $stage = $pu01->{stage};
    my $output_no = $pu01->{output_no};
    #
    my $pcols = $pu01->{$section}->{column_names};
    #
    foreach my $prow (@{$pu01->{$section}->{data}})
    {
        my $nhadd = $prow->{NHAdd};
        my $ncadd = $prow->{NCAdd};
        #
        foreach my $col (@{$pcols})
        {
            $pdb->{$section}->{$machine}{$lane}{$stage}{cache}{$nhadd}{$ncadd}{data}{$col} = $prow->{$col};
        }
        #
        $pdb->{$section}->{$machine}{$lane}{$stage}{cache}{$nhadd}{$ncadd}{state} = $state;
    }
}
#
sub copy_u01_nozzle_delta
{
    my ($pdb, $pu01) = @_;
    #
    my $section = MOUNTPICKUPNOZZLE;
    #
    my $filename = $pu01->{file_name};
    my $machine = $pu01->{mach_no};
    my $lane = $pu01->{lane};
    my $stage = $pu01->{stage};
    my $output_no = $pu01->{output_no};
    #
    my $pcols = $pu01->{$section}->{column_names};
    #
    delete $pdb->{$section}->{$machine}{$lane}{$stage}{delta};
    #
    foreach my $prow (@{$pu01->{$section}->{data}})
    {
        my $nhadd = $prow->{NHAdd};
        my $ncadd = $prow->{NCAdd};
        #
        foreach my $col (@{$pcols})
        {
            $pdb->{$section}->{$machine}{$lane}{$stage}{delta}{$nhadd}{$ncadd}{data}{$col} = $prow->{$col};
        }
    }
}
#
sub tabulate_u01_nozzle_delta
{
    my ($pdb, $pu01) = @_;
    #
    my $filename = $pu01->{file_name};
    #
    my $machine = $pu01->{mach_no};
    my $lane = $pu01->{lane};
    my $stage = $pu01->{stage};
    my $output_no = $pu01->{output_no};
    my $section = MOUNTPICKUPNOZZLE;
    #
    my $product = $pdb->{product}{u01}{$machine}{$lane}{$stage};
    #
    foreach my $nhadd (sort { $a <=> $b } keys %{$pdb->{$section}{$machine}{$lane}{$stage}{delta}})
    {
        foreach my $ncadd (sort { $a <=> $b } keys %{$pdb->{$section}{$machine}{$lane}{$stage}{delta}{$nhadd}})
        {
            my $blkserial = $pdb->{$section}{$machine}{$lane}{$stage}{delta}{$nhadd}{$ncadd}{data}{BLKSerial};
            #
            # product-independent totals
            #
            if (exists($totals{$section}{by_machine_lane_stage_nhadd_ncadd_blkserial}{$machine}{$lane}{$stage}{$nhadd}{$ncadd}{$blkserial}))
            {
                foreach my $col (@nozzle_count_cols)
                {
                    $totals{$section}{by_machine_lane_stage_nhadd_ncadd_blkserial}{$machine}{$lane}{$stage}{$nhadd}{$ncadd}{$blkserial}{$col} += $pdb->{$section}{$machine}{$lane}{$stage}{delta}{$nhadd}{$ncadd}{data}{$col};
                }
            }
            else
            {
                foreach my $col (@nozzle_count_cols)
                {
                    $totals{$section}{by_machine_lane_stage_nhadd_ncadd_blkserial}{$machine}{$lane}{$stage}{$nhadd}{$ncadd}{$blkserial}{$col} = $pdb->{$section}{$machine}{$lane}{$stage}{delta}{$nhadd}{$ncadd}{data}{$col};
                }
            }
            #
            if (exists($totals{$section}{by_machine_lane_stage_nhadd_ncadd}{$machine}{$lane}{$stage}{$nhadd}{$ncadd}))
            {
                foreach my $col (@nozzle_count_cols)
                {
                    $totals{$section}{by_machine_lane_stage_nhadd_ncadd}{$machine}{$lane}{$stage}{$nhadd}{$ncadd}{$col} += $pdb->{$section}{$machine}{$lane}{$stage}{delta}{$nhadd}{$ncadd}{data}{$col};
                }
            }
            else
            {
                foreach my $col (@nozzle_count_cols)
                {
                    $totals{$section}{by_machine_lane_stage_nhadd_ncadd}{$machine}{$lane}{$stage}{$nhadd}{$ncadd}{$col} = $pdb->{$section}{$machine}{$lane}{$stage}{delta}{$nhadd}{$ncadd}{data}{$col};
                }
            }
            #
            # product-dependent totals
            #
            if (exists($totals{$section}{by_product}{$product}{by_machine_lane_stage_nhadd_ncadd_blkserial}{$machine}{$lane}{$stage}{$nhadd}{$ncadd}{$blkserial}))
            {
                foreach my $col (@nozzle_count_cols)
                {
                    $totals{$section}{by_product}{$product}{by_machine_lane_stage_nhadd_ncadd_blkserial}{$machine}{$lane}{$stage}{$nhadd}{$ncadd}{$blkserial}{$col} += $pdb->{$section}{$machine}{$lane}{$stage}{delta}{$nhadd}{$ncadd}{data}{$col};
                }
            }
            else
            {
                foreach my $col (@nozzle_count_cols)
                {
                    $totals{$section}{by_product}{$product}{by_machine_lane_stage_nhadd_ncadd_blkserial}{$machine}{$lane}{$stage}{$nhadd}{$ncadd}{$blkserial}{$col} = $pdb->{$section}{$machine}{$lane}{$stage}{delta}{$nhadd}{$ncadd}{data}{$col};
                }
            }
            #
            if (exists($totals{$section}{by_product}{$product}{by_machine_lane_stage_nhadd_ncadd}{$machine}{$lane}{$stage}{$nhadd}{$ncadd}))
            {
                foreach my $col (@nozzle_count_cols)
                {
                    $totals{$section}{by_product}{$product}{by_machine_lane_stage_nhadd_ncadd}{$machine}{$lane}{$stage}{$nhadd}{$ncadd}{$col} += $pdb->{$section}{$machine}{$lane}{$stage}{delta}{$nhadd}{$ncadd}{data}{$col};
                }
            }
            else
            {
                foreach my $col (@nozzle_count_cols)
                {
                    $totals{$section}{by_product}{$product}{by_machine_lane_stage_nhadd_ncadd}{$machine}{$lane}{$stage}{$nhadd}{$ncadd}{$col} = $pdb->{$section}{$machine}{$lane}{$stage}{delta}{$nhadd}{$ncadd}{data}{$col};
                }
            }
        }
    }
}
#
sub audit_u01_nozzles
{
    my ($pdb, $pu01) = @_;
    #
    my $machine = $pu01->{mach_no};
    my $lane = $pu01->{lane};
    my $stage = $pu01->{stage};
    my $output_no = $pu01->{output_no};
    my $section = MOUNTPICKUPNOZZLE;
    my $filename = $pu01->{file_name};
    #
    set_list_section_column_names(LNB_U01_FILE_TYPE, $pu01, $section);
    #
    printf $log_fh "\n%d: SECTION  : %s\n", __LINE__, $section
        if ($verbose >= MAXVERBOSE);
    #
    if ($verbose >= MAXVERBOSE)
    {
        printf $log_fh "%d: MACHINE  : %s\n", __LINE__, $machine;
        printf $log_fh "%d: LANE     : %d\n", __LINE__, $lane;
        printf $log_fh "%d: STAGE    : %d\n", __LINE__, $stage;
        printf $log_fh "%d: OUTPUT NO: %s\n", __LINE__, $output_no;
        printf $log_fh "%d: FILE RECS : %d\n", __LINE__, scalar(@{$pu01->{data}});
        printf $log_fh "%d: %s RECS: %d\n", __LINE__, $section, scalar(@{$pu01->{$section}->{data}}) if (defined(@{$pu01->{$section}->{data}}));
    }
    #
    # check if the file has a nozzle data section.
    #
    if (($output_no == DETECT_CHANGE) ||
        ($output_no == TIMER_NOT_RUNNING))
    {
        printf $log_fh "%d: No Nozzle data in Output=%d U01 files. Skipping.\n", __LINE__, $output_no if ($verbose >= MAXVERBOSE);
        return;
    }
    elsif (($output_no == PROD_COMPLETE) ||
           ($output_no == PROD_COMPLETE_LATER))
    {
        if ( ! exists($pdb->{$section}->{$machine}{$lane}{$stage}{state}))
        {
            printf $log_fh "%d: ENTRY STATE: UNKNOWN\n", __LINE__,
                if ($verbose >= MAXVERBOSE);
            #
            delete $pdb->{$section}->{$machine}{$lane}{$stage}{cache};
            copy_u01_nozzle_cache($pdb, $pu01, DELTA);
            #
            $pdb->{$section}->{$machine}{$lane}{$stage}{state} = DELTA;
        }
        elsif ($pdb->{$section}->{$machine}{$lane}{$stage}{state} eq RESET)
        {
            printf $log_fh "%d: ENTRY STATE: %s\n", __LINE__,
                $pdb->{$section}->{$machine}{$lane}{$stage}{state}
                if ($verbose >= MAXVERBOSE);
            copy_u01_nozzle_delta($pdb, $pu01);
            tabulate_u01_nozzle_delta($pdb, $pu01);
            copy_u01_nozzle_cache($pdb, $pu01, DELTA);
            #
            $pdb->{$section}->{$machine}{$lane}{$stage}{state} = DELTA;
        }
        elsif ($pdb->{$section}->{$machine}{$lane}{$stage}{state} eq DELTA)
        {
            printf $log_fh "%d: ENTRY STATE: %s\n", __LINE__,
                $pdb->{$section}->{$machine}{$lane}{$stage}{state}
                if ($verbose >= MAXVERBOSE);
            calculate_u01_nozzle_delta($pdb, $pu01);
            tabulate_u01_nozzle_delta($pdb, $pu01);
            copy_u01_nozzle_cache($pdb, $pu01, DELTA);
            #
            $pdb->{$section}->{$machine}{$lane}{$stage}{state} = DELTA;
        }
        elsif ($pdb->{$section}->{$machine}{$lane}{$stage}{state} eq BASELINE)
        {
            printf $log_fh "%d: ENTRY STATE: %s\n", __LINE__,
                $pdb->{$section}->{$machine}{$lane}{$stage}{state}
                if ($verbose >= MAXVERBOSE);
            #
            delete $pdb->{$section}->{$machine}{$lane}{$stage}{cache};
            copy_u01_nozzle_cache($pdb, $pu01, DELTA);
            #
            $pdb->{$section}->{$machine}{$lane}{$stage}{state} = DELTA;
        }
        else
        {
            my $state = $pdb->{$section}->{$machine}{$lane}{$stage}{state};
            die "ERROR: unknown $section state: $state. Stopped";
        }
    }
    elsif (($output_no == MANUAL_CLEAR) ||
           ($output_no == AUTO_CLEAR))
    {
        printf $log_fh "%d: ENTRY STATE: %s\n", __LINE__,
            $pdb->{$section}->{$machine}{$lane}{$stage}{state}
            if ($verbose >= MAXVERBOSE);
        $pdb->{$section}->{$machine}{$lane}{$stage}{state} = RESET;
        delete $pdb->{$section}->{$machine}{$lane}{$stage}{cache};
    }
    else
    {
        die "ERROR: unknown $section output type: $output_no. Stopped";
    }
    #
    printf $log_fh "%d: EXIT STATE: %s\n", __LINE__,
        $pdb->{$section}->{$machine}{$lane}{$stage}{state}
        if ($verbose >= MAXVERBOSE);
    #
    return;
}
#
######################################################################
#
# routines for nozzle section
#
sub calculate_u01_nozzle_delta_keys
{
    my ($pdb, $pu01, $nmkey1, $nmkey2, $label) = @_;
    #
    my $section = MOUNTPICKUPNOZZLE;
    #
    my $filename = $pu01->{file_name};
    my $machine = $pu01->{mach_no};
    my $lane = $pu01->{lane};
    my $stage = $pu01->{stage};
    my $output_no = $pu01->{output_no};
    #
    my $pcols = $pu01->{$section}->{column_names};
    #
    delete $pdb->{$section}->{$label}->{$machine}{$lane}{$stage}{delta};
    #
    foreach my $prow (@{$pu01->{$section}->{data}})
    {
        my $key1 = $prow->{$nmkey1};
        my $key2 = $prow->{$nmkey2};
        my $blkserial = $prow->{BLKSerial};
        #
        if ( ! exists($pdb->{$section}->{$label}->{$machine}{$lane}{$stage}{cache}{$key1}{$key2}{data}))
        {
            printf $log_fh "%d: WARNING: [%s] %s $nmkey2 %s, $nmkey2 %s NOT found in cache. Taking all counts as is.\n", __LINE__, $filename, $section, $key1, $key2 if ($verbose >= MINVERBOSE);
            foreach my $col (@{$pcols})
            {
                $pdb->{$section}->{$label}->{$machine}{$lane}{$stage}{delta}{$key1}{$key2}{data}{$col} = $prow->{$col};
            }
        }
        else
        {
            my $cache_blkserial = $pdb->{$section}->{$label}->{$machine}{$lane}{$stage}{cache}{$key1}{$key2}{data}{BLKSerial};
            if ($blkserial eq $cache_blkserial)
            {
                $pdb->{$section}->{$label}->{$machine}{$lane}{$stage}{delta}{$key1}{$key2}{data}{BLKSerial} = $blkserial;
                #
                foreach my $col (@nozzle_count_cols)
                {
                    my $u01_value = $prow->{$col};
                    my $cache_value = $pdb->{$section}->{$label}->{$machine}{$lane}{$stage}{cache}{$key1}{$key2}{data}{$col};
                    #
                    my $delta = $u01_value - $cache_value;
                    #
                    if ($delta >= 0)
                    {
                        $pdb->{$section}->{$label}->{$machine}{$lane}{$stage}{delta}{$key1}{$key2}{data}{$col} = $delta;
                    }
                    elsif (($proc_options & PROC_OPT_USENEGDELTS) != 0)
                    {
                         printf $log_fh "%d: WARNING: [%s] %s $nmkey1 %s, $nmkey2 %s using NEGATIVE delta for key %s: %d\n", __LINE__, $filename, $section, $key1, $key2, $col, $delta if ($verbose >= MINVERBOSE);
                        $pdb->{$section}->{$label}->{$machine}{$lane}{$stage}{delta}{$key1}{$key2}{data}{$col} = $delta;
                    }
                    else
                    {
                        $pdb->{$section}->{$label}->{$machine}{$lane}{$stage}{delta}{$key1}{$key2}{data}{$col} = 0;
                         printf $log_fh "%d: WARNING: [%s] %s $nmkey1 %s, $nmkey2 %s setting NEGATIVE delta (%d) for key %s to ZERO\n", __LINE__, $filename, $section, $key1, $key2, $delta, $col if ($verbose >= MINVERBOSE);
                    }
                }
            }
            else
            {
                printf $log_fh "%d: WARNING: [%s] %s $nmkey1 %s, $nmkey2 %s BLKSERIAL CHANGED: CACHED %s, CURRENT U01 %s\n", __LINE__, $filename, $section, $key1, $key2, $cache_blkserial, $blkserial if ($verbose >= MINVERBOSE);
                #
                delete $pdb->{$section}->{$label}->{$machine}{$lane}{$stage}{delta}{$key1}{$key2}{data};
                #
                foreach my $col (@{$pcols})
                {
                    $pdb->{$section}->{$label}->{$machine}{$lane}{$stage}{delta}{$key1}{$key2}{data}{$col} = $prow->{$col};
                }
            }
        }
    }
}
#
sub copy_u01_nozzle_cache_keys
{
    my ($pdb, $pu01, $state, $nmkey1, $nmkey2, $label) = @_;
    #
    my $section = MOUNTPICKUPNOZZLE;
    #
    my $filename = $pu01->{file_name};
    my $machine = $pu01->{mach_no};
    my $lane = $pu01->{lane};
    my $stage = $pu01->{stage};
    my $output_no = $pu01->{output_no};
    #
    my $pcols = $pu01->{$section}->{column_names};
    #
    foreach my $prow (@{$pu01->{$section}->{data}})
    {
        my $key1 = $prow->{$nmkey1};
        my $key2 = $prow->{$nmkey2};
# printf $log_fh "%d: $label $nmkey1 %d $nmkey2 %d\n", __LINE__, $key1, $key2;
        #
        foreach my $col (@{$pcols})
        {
            $pdb->{$section}->{$label}->{$machine}{$lane}{$stage}{cache}{$key1}{$key2}{data}{$col} = $prow->{$col};
# printf $log_fh "%d: $label $nmkey1 %d $nmkey2 %d $col %s\n", __LINE__, $key1, $key2, $prow->{$col}
        }
        #
        $pdb->{$section}->{$label}->{$machine}{$lane}{$stage}{cache}{$key1}{$key2}{state} = $state;
    }
}
#
sub copy_u01_nozzle_delta_keys
{
    my ($pdb, $pu01, $nmkey1, $nmkey2, $label) = @_;
    #
    my $section = MOUNTPICKUPNOZZLE;
    #
    my $filename = $pu01->{file_name};
    my $machine = $pu01->{mach_no};
    my $lane = $pu01->{lane};
    my $stage = $pu01->{stage};
    my $output_no = $pu01->{output_no};
    #
    my $pcols = $pu01->{$section}->{column_names};
    #
    delete $pdb->{$section}->{$label}->{$machine}{$lane}{$stage}{delta};
    #
    foreach my $prow (@{$pu01->{$section}->{data}})
    {
        my $key1 = $prow->{$nmkey1};
        my $key2 = $prow->{$nmkey2};
        #
        foreach my $col (@{$pcols})
        {
            $pdb->{$section}->{$label}->{$machine}{$lane}{$stage}{delta}{$key1}{$key2}{data}{$col} = $prow->{$col};
        }
    }
}
#
sub tabulate_u01_nozzle_delta_keys
{
    my ($pdb, $pu01, $nmkey1, $nmkey2, $label) = @_;
    #
    my $filename = $pu01->{file_name};
    #
    my $machine = $pu01->{mach_no};
    my $lane = $pu01->{lane};
    my $stage = $pu01->{stage};
    my $output_no = $pu01->{output_no};
    my $section = MOUNTPICKUPNOZZLE;
    #
    my $product = $pdb->{product}{u01}{$machine}{$lane}{$stage};
    #
    foreach my $key1 (sort { $a <=> $b } keys %{$pdb->{$section}->{$label}->{$machine}{$lane}{$stage}{delta}})
    {
        foreach my $key2 (sort { $a <=> $b } keys %{$pdb->{$section}->{$label}->{$machine}{$lane}{$stage}{delta}{$key1}})
        {
            my $blkserial = $pdb->{$section}->{$label}->{$machine}{$lane}{$stage}{delta}{$key1}{$key2}{data}{BLKSerial};
            #
            # product-independent totals
            #
            if (exists($totals{$section}{$label}{by_machine_lane_stage_key1_key2_blkserial}{$machine}{$lane}{$stage}{$key1}{$key2}{$blkserial}))
            {
                foreach my $col (@nozzle_count_cols)
                {
                    $totals{$section}{$label}{by_machine_lane_stage_key1_key2_blkserial}{$machine}{$lane}{$stage}{$key1}{$key2}{$blkserial}{$col} += $pdb->{$section}->{$label}->{$machine}{$lane}{$stage}{delta}{$key1}{$key2}{data}{$col};
                }
            }
            else
            {
                foreach my $col (@nozzle_count_cols)
                {
                    $totals{$section}{$label}{by_machine_lane_stage_key1_key2_blkserial}{$machine}{$lane}{$stage}{$key1}{$key2}{$blkserial}{$col} = $pdb->{$section}->{$label}->{$machine}{$lane}{$stage}{delta}{$key1}{$key2}{data}{$col};
                }
            }
            #
            if (exists($totals{$section}{$label}{by_machine_lane_stage_key1_key2}{$machine}{$lane}{$stage}{$key1}{$key2}))
            {
                foreach my $col (@nozzle_count_cols)
                {
                    $totals{$section}{$label}{by_machine_lane_stage_key1_key2}{$machine}{$lane}{$stage}{$key1}{$key2}{$col} += $pdb->{$section}->{$label}->{$machine}{$lane}{$stage}{delta}{$key1}{$key2}{data}{$col};
                }
            }
            else
            {
                foreach my $col (@nozzle_count_cols)
                {
                    $totals{$section}{$label}{by_machine_lane_stage_key1_key2}{$machine}{$lane}{$stage}{$key1}{$key2}{$col} = $pdb->{$section}->{$label}->{$machine}{$lane}{$stage}{delta}{$key1}{$key2}{data}{$col};
                }
            }
            #
            # product-dependent totals
            #
            if (exists($totals{$section}{by_product}{$product}{$label}{by_machine_lane_stage_key1_key2_blkserial}{$machine}{$lane}{$stage}{$key1}{$key2}{$blkserial}))
            {
                foreach my $col (@nozzle_count_cols)
                {
                    $totals{$section}{by_product}{$product}{$label}{by_machine_lane_stage_key1_key2_blkserial}{$machine}{$lane}{$stage}{$key1}{$key2}{$blkserial}{$col} += $pdb->{$section}->{$label}->{$machine}{$lane}{$stage}{delta}{$key1}{$key2}{data}{$col};
                }
            }
            else
            {
                foreach my $col (@nozzle_count_cols)
                {
                    $totals{$section}{by_product}{$product}{$label}{by_machine_lane_stage_key1_key2_blkserial}{$machine}{$lane}{$stage}{$key1}{$key2}{$blkserial}{$col} = $pdb->{$section}->{$label}->{$machine}{$lane}{$stage}{delta}{$key1}{$key2}{data}{$col};
                }
            }
            #
            if (exists($totals{$section}{by_product}{$product}{$label}{by_machine_lane_stage_key1_key2}{$machine}{$lane}{$stage}{$key1}{$key2}))
            {
                foreach my $col (@nozzle_count_cols)
                {
                    $totals{$section}{by_product}{$product}{$label}{by_machine_lane_stage_key1_key2}{$machine}{$lane}{$stage}{$key1}{$key2}{$col} += $pdb->{$section}->{$label}->{$machine}{$lane}{$stage}{delta}{$key1}{$key2}{data}{$col};
                }
            }
            else
            {
                foreach my $col (@nozzle_count_cols)
                {
                    $totals{$section}{by_product}{$product}{$label}{by_machine_lane_stage_key1_key2}{$machine}{$lane}{$stage}{$key1}{$key2}{$col} = $pdb->{$section}->{$label}->{$machine}{$lane}{$stage}{delta}{$key1}{$key2}{data}{$col};
                }
            }
        }
    }
}
#
sub audit_u01_nozzles_keys
{
    my ($pdb, $pu01, $nmkey1, $nmkey2, $label) = @_;
    #
    my $machine = $pu01->{mach_no};
    my $lane = $pu01->{lane};
    my $stage = $pu01->{stage};
    my $output_no = $pu01->{output_no};
    my $section = MOUNTPICKUPNOZZLE;
    my $filename = $pu01->{file_name};
    #
    printf $log_fh "\n%d: SECTION  : %s\n", __LINE__, $section
        if ($verbose >= MAXVERBOSE);
    #
    if ($verbose >= MAXVERBOSE)
    {
        printf $log_fh "%d: MACHINE  : %s\n", __LINE__, $machine;
        printf $log_fh "%d: LANE     : %d\n", __LINE__, $lane;
        printf $log_fh "%d: STAGE    : %d\n", __LINE__, $stage;
        printf $log_fh "%d: OUTPUT NO: %s\n", __LINE__, $output_no;
        printf $log_fh "%d: FILE RECS : %d\n", __LINE__, scalar(@{$pu01->{data}});
        printf $log_fh "%d: %s RECS: %d\n", __LINE__, $section, scalar(@{$pu01->{$section}->{data}}) if (defined(@{$pu01->{$section}->{data}}));
    }
    #
    # check if the file has a nozzle data section.
    #
    if (($output_no == DETECT_CHANGE) ||
        ($output_no == TIMER_NOT_RUNNING))
    {
        printf $log_fh "%d: No Nozzle data in Output=%d U01 files. Skipping.\n", __LINE__, $output_no if ($verbose >= MAXVERBOSE);
        return;
    }
    elsif (($output_no == PROD_COMPLETE) ||
           ($output_no == PROD_COMPLETE_LATER))
    {
        if ( ! exists($pdb->{$section}->{$label}->{$machine}{$lane}{$stage}{state}))
        {
            printf $log_fh "%d: ENTRY STATE: UNKNOWN\n", __LINE__
                if ($verbose >= MAXVERBOSE);
            #
            delete $pdb->{$section}->{$label}->{$machine}{$lane}{$stage}{cache};
            copy_u01_nozzle_cache_keys(
                $pdb, $pu01, DELTA, $nmkey1, $nmkey2, $label);
            #
            $pdb->{$section}->{$label}->{$machine}{$lane}{$stage}{state} = DELTA;
        }
        elsif ($pdb->{$section}->{$label}->{$machine}{$lane}{$stage}{state} eq RESET)
        {
            printf $log_fh "%d: ENTRY STATE: %s\n", __LINE__,
                $pdb->{$section}->{$label}->{$machine}{$lane}{$stage}{state}
                if ($verbose >= MAXVERBOSE);
            copy_u01_nozzle_delta_keys(
                $pdb, $pu01, $nmkey1, $nmkey2, $label);
            tabulate_u01_nozzle_delta_keys(
                $pdb, $pu01, $nmkey1, $nmkey2, $label);
            copy_u01_nozzle_cache_keys(
                $pdb, $pu01, DELTA, $nmkey1, $nmkey2, $label);
            #
            $pdb->{$section}->{$label}->{$machine}{$lane}{$stage}{state} = DELTA;
        }
        elsif ($pdb->{$section}->{$label}->{$machine}{$lane}{$stage}{state} eq DELTA)
        {
            printf $log_fh "%d: ENTRY STATE: %s\n", __LINE__,
                $pdb->{$section}->{$label}->{$machine}{$lane}{$stage}{state}
                if ($verbose >= MAXVERBOSE);
            calculate_u01_nozzle_delta_keys(
                $pdb, $pu01, $nmkey1, $nmkey2, $label);
            tabulate_u01_nozzle_delta_keys(
                $pdb, $pu01, $nmkey1, $nmkey2, $label);
            copy_u01_nozzle_cache_keys(
                $pdb, $pu01, DELTA, $nmkey1, $nmkey2, $label);
            #
            $pdb->{$section}->{$label}->{$machine}{$lane}{$stage}{state} = DELTA;
        }
        elsif ($pdb->{$section}->{$label}->{$machine}{$lane}{$stage}{state} eq BASELINE)
        {
            printf $log_fh "%d: ENTRY STATE: %s\n", __LINE__,
                $pdb->{$section}->{$machine}{$lane}{$stage}{state}
                if ($verbose >= MAXVERBOSE);
            #
            delete $pdb->{$section}->{$label}->{$machine}{$lane}{$stage}{cache};
            copy_u01_nozzle_cache_keys(
                $pdb, $pu01, DELTA, $nmkey1, $nmkey2, $label);
            #
            $pdb->{$section}->{$label}->{$machine}{$lane}{$stage}{state} = DELTA;
        }
        else
        {
            my $state = $pdb->{$section}->{$label}->{$machine}{$lane}{$stage}{state};
            die "ERROR: unknown $section state: $state. Stopped";
        }
    }
    elsif (($output_no == MANUAL_CLEAR) ||
           ($output_no == AUTO_CLEAR))
    {
        printf $log_fh "%d: ENTRY STATE: %s\n", __LINE__,
            $pdb->{$section}->{$label}->{$machine}{$lane}{$stage}{state}
            if ($verbose >= MAXVERBOSE);
        $pdb->{$section}->{$label}->{$machine}{$lane}{$stage}{state} = RESET;
        delete $pdb->{$section}->{$label}->{$machine}{$lane}{$stage}{cache};
    }
    else
    {
        die "ERROR: unknown $section output type: $output_no. Stopped";
    }
    #
    printf $log_fh "%d: EXIT STATE: %s\n", __LINE__,
        $pdb->{$section}->{$label}->{$machine}{$lane}{$stage}{state}
        if ($verbose >= MAXVERBOSE);
    #
    return;
}
#
sub audit_u01_nozzles_new
{
    my ($pdb, $pu01) = @_;
    #
    audit_u01_nozzles_keys($pdb, $pu01, 
                           NZ_KEY_NHADD, 
                           NZ_KEY_NCADD, 
                           NZ_LABEL_NHADD_NCADD);
    audit_u01_nozzles_keys($pdb, $pu01, 
                           NZ_KEY_HEAD, 
                           NZ_KEY_NHADD, 
                           NZ_LABEL_HEAD_NHADD);
    audit_u01_nozzles_keys($pdb, $pu01, 
                           NZ_KEY_HEAD, 
                           NZ_KEY_NCADD, 
                           NZ_LABEL_HEAD_NCADD);
}
#
#####################################################################
#
# high-level audit functions for u01 files.
#
sub audit_u01_file
{
    my ($pdb, $pu01) = @_;
    #
    my $output_no = $pu01->{output_no};
    #
    return if (($output_no == TIMER_NOT_RUNNING) &&
               (($proc_options & PROC_OPT_IGNALL12) != 0));
    #
    set_product_info($pdb, $pu01, LNB_U01_FILE_TYPE);
    #
    audit_u01_name_value($pdb, $pu01, COUNT);
    audit_u01_name_value($pdb, $pu01, TIME);
    audit_u01_feeders($pdb, $pu01);
    #
    if (($proc_options & PROC_OPT_USEOLDNZ) != 0)
    {
        audit_u01_nozzles($pdb, $pu01);
    }
    else
    {
        audit_u01_nozzles_new($pdb, $pu01);
    }
    #
    return;
}
#
sub load_u01_sections
{
    my ($pu01) = @_;
    #
    load_name_value($pu01, INDEX);
    load_name_value($pu01, INFORMATION);
    #
    load_name_value($pu01, TIME);
    load_name_value($pu01, CYCLETIME);
    load_name_value($pu01, COUNT);
    load_list($pu01, DISPENSER);
    load_list($pu01, MOUNTPICKUPFEEDER);
    # backfill_list($pu01, MOUNTPICKUPFEEDER, \@feeder_count_cols);
    load_list($pu01, MOUNTPICKUPNOZZLE);
    # backfill_list($pu01, MOUNTPICKUPNOZZLE, \@nozzle_count_cols);
    load_name_value($pu01, INSPECTIONDATA);
}
#
sub audit_u01_files
{
    my ($pu01s, $pdb) = @_;
    #
    printf $log_fh "\n%d: Audit U01 files:\n", __LINE__;
    #
    foreach my $pu01 (@{$pu01s})
    {
        printf $log_fh "\n%d: Audit U01: %s\n", __LINE__, $pu01->{file_name} if ($verbose >= MIDVERBOSE);
        #
        next unless (load($pu01) != 0);
        #
        load_u01_sections($pu01);
        #
        audit_u01_file($pdb, $pu01);
    }
    #
    return;
}
#
########################################################################
########################################################################
#
# process U01 files for csv export.
#
sub export_u01_data_as_csv
{
    my ($pdb) = @_;
    #
    export_name_value_section_as_csv(TIME,
                                     LNB_U01_FILE_TYPE, 
                                    'TIME',
                                    'machine',
                                     TRUE);
    export_name_value_section_as_csv(CYCLETIME,
                                     LNB_U01_FILE_TYPE, 
                                    'CYCLE_TIME',
                                    'machine',
                                     TRUE);
    export_name_value_section_as_csv(COUNT,
                                     LNB_U01_FILE_TYPE, 
                                    'COUNT',
                                    'machine',
                                     TRUE);
    #
    export_list_section_as_csv(DISPENSER,
                               LNB_U01_FILE_TYPE, 
                              'DISPENSER',
                              'machine',
                               TRUE);
    export_list_section_as_csv(MOUNTPICKUPFEEDER,
                               LNB_U01_FILE_TYPE, 
                              'MOUNT_PICKUP_FEEDER',
                              'machine',
                               TRUE);
    export_list_section_as_csv(MOUNTPICKUPNOZZLE,
                               LNB_U01_FILE_TYPE, 
                              'MOUNT_PICKUP_NOZZLE',
                              'machine',
                               TRUE);
    export_name_value_section_as_csv(INSPECTIONDATA,
                                     LNB_U01_FILE_TYPE, 
                                    'INSPECTION_DATA',
                                    'machine',
                                     TRUE);
}
#
sub prepare_u01_file
{
    my ($pdb, $pu01) = @_;
    #
    set_product_info($pdb, $pu01, LNB_U01_FILE_TYPE);
    #
    prepare_name_value_section($pdb, 
                               $pu01, 
                               LNB_U01_FILE_TYPE, 
                               TIME,
                               TRUE);
    prepare_name_value_section($pdb, 
                               $pu01, 
                               LNB_U01_FILE_TYPE, 
                               CYCLETIME,
                               TRUE);
    prepare_name_value_section($pdb, 
                               $pu01, 
                               LNB_U01_FILE_TYPE, 
                               COUNT,
                               TRUE);
    prepare_list_section($pdb, 
                         $pu01, 
                         LNB_U01_FILE_TYPE, 
                         DISPENSER,
                         TRUE);
    prepare_list_section($pdb, 
                         $pu01, 
                         LNB_U01_FILE_TYPE, 
                         MOUNTPICKUPFEEDER,
                         TRUE);
    prepare_list_section($pdb, 
                         $pu01, 
                         LNB_U01_FILE_TYPE, 
                         MOUNTPICKUPNOZZLE,
                         TRUE);
    prepare_name_value_section($pdb, 
                               $pu01, 
                               LNB_U01_FILE_TYPE, 
                               INSPECTIONDATA,
                               TRUE);
    #
    return;
}
#
sub prepare_u01_files
{
    my ($pu01s, $pdb) = @_;
    #
    printf $log_fh "\n%d: Audit U01 files:\n", __LINE__;
    #
    foreach my $pu01 (@{$pu01s})
    {
        printf $log_fh "\n%d: Audit u01: %s\n", __LINE__, $pu01->{file_name}
            if ($verbose >= MIDVERBOSE);
        #
        next unless (load($pu01) != 0);
        #
        load_u01_sections($pu01);
        #
        prepare_u01_file($pdb, $pu01);
    }
    #
    return;
}
#
sub process_u01_files
{
    my ($pu01s) = @_;
    #
    # any files to process?
    #
    if (scalar(@{$pu01s}) <= 0)
    {
        printf $log_fh "%d: No U01 files to process. Returning.\n\n", __LINE__;
        return;
    }
    #
    my %db = ();
    audit_u01_files($pu01s, \%db);
    export_u01_data(\%db);
    #
    my %csv_db = ();
    prepare_u01_files($pu01s, \%csv_db);
    export_u01_data_as_csv(\%csv_db);
    #
    return;
}
#
########################################################################
########################################################################
#
# process U03 files.
#
sub export_u03_data_as_csv
{
    my ($pdb) = @_;
    #
    export_list_section_as_csv(MOUNTQUALITYTRACE, 
                               LNB_U03_FILE_TYPE(), 
                              'MOUNT_QUALITY_TRACE', 
                              'machine',
                               TRUE);
    export_list_section_as_csv(MOUNTLATESTREEL, 
                               LNB_U03_FILE_TYPE(), 
                              'MOUNT_LATEST_REEL', 
                              'machine',
                               TRUE);
    export_list_section_as_csv(MOUNTEXCHANGEREEL, 
                               LNB_U03_FILE_TYPE(), 
                              'MOUNT_EXCHANGE_REEL', 
                              'machine',
                               TRUE);
}
#
sub prepare_u03_file
{
    my ($pdb, $pu03) = @_;
    #
    set_product_info($pdb, $pu03, LNB_U03_FILE_TYPE);
    #
    prepare_list_section($pdb, 
                         $pu03, 
                         LNB_U03_FILE_TYPE, 
                         MOUNTQUALITYTRACE,
                         TRUE);
    prepare_list_section($pdb, 
                         $pu03, 
                         LNB_U03_FILE_TYPE, 
                         MOUNTLATESTREEL,
                         TRUE);
    prepare_list_section($pdb, 
                         $pu03, 
                         LNB_U03_FILE_TYPE, 
                         MOUNTEXCHANGEREEL,
                         TRUE);
    #
    return;
}
#
sub load_u03_sections
{
    my ($pu03) = @_;
    #
    load_name_value($pu03, INDEX);
    load_name_value($pu03, INFORMATION);
    #
    load_list($pu03, BRECG);
    load_list($pu03, BRECGCALC);
    load_list($pu03, ELAPSETIMERECOG);
    load_list($pu03, SBOARD);
    load_list($pu03, HEIGHTCORRECT);
    load_list($pu03, MOUNTQUALITYTRACE);
    load_list($pu03, MOUNTLATESTREEL);
    load_list($pu03, MOUNTEXCHANGEREEL);
}
#
sub prepare_u03_files
{
    my ($pu03s, $pdb) = @_;
    #
    printf $log_fh "\n%d: Audit U03 files:\n", __LINE__;
    #
    foreach my $pu03 (@{$pu03s})
    {
        printf $log_fh "\n%d: Audit u03: %s\n", __LINE__, $pu03->{file_name}
            if ($verbose >= MIDVERBOSE);
        #
        next unless (load($pu03) != 0);
        #
        load_u03_sections($pu03);
        #
        prepare_u03_file($pdb, $pu03);
    }
    #
    return;
}
#
sub process_u03_files
{
    my ($pu03s) = @_;
    #
    # any files to process?
    #
    if (scalar(@{$pu03s}) <= 0)
    {
        printf $log_fh "\n%d: No U03 files to process. Returning.\n\n", __LINE__;
        return;
    }
    #
    my %csv_db = ();
    prepare_u03_files($pu03s, \%csv_db);
    export_u03_data_as_csv(\%csv_db);
    #
    return;
}
#
########################################################################
########################################################################
#
# process MPR files.
#
sub export_mpr_data_as_csv
{
    my ($pdb) = @_;
    #
    export_list_section_as_csv(TIMEDATASP, 
                               LNB_MPR_FILE_TYPE(), 
                              'TIME_DATA_SP', 
                              'sp',
                               FALSE);
    export_list_section_as_csv(COUNTDATASP, 
                               LNB_MPR_FILE_TYPE(), 
                              'COUNT_DATA_SP', 
                              'sp',
                               FALSE);
    export_list_section_as_csv(COUNTDATASP2, 
                               LNB_MPR_FILE_TYPE(), 
                              'COUNT_DATA_SP2', 
                              'sp',
                               FALSE);
    export_list_section_as_csv(TRACEDATASP, 
                               LNB_MPR_FILE_TYPE(), 
                              'TRACE_DATA_SP', 
                              'sp',
                               FALSE);
    export_list_section_as_csv(TRACEDATASP_2, 
                               LNB_MPR_FILE_TYPE(), 
                              'TRACE_DATA_SP_2', 
                              'sp',
                               FALSE);
    export_list_section_as_csv(ISPINFODATA, 
                               LNB_MPR_FILE_TYPE(), 
                              'ISP_INFO_DATA', 
                              'sp',
                               FALSE);
    export_list_section_as_csv(MASKISPINFODATA, 
                        LNB_MPR_FILE_TYPE(), 
                       'MASK_ISP_INFO_DATA', 
                       'sp',
                        FALSE);
}
#
sub prepare_mpr_file
{
    my ($pdb, $pmpr) = @_;
    #
    prepare_list_section($pdb, 
                         $pmpr, 
                         LNB_MPR_FILE_TYPE, 
                         TIMEDATASP, 
                         FALSE);
    prepare_list_section($pdb, 
                         $pmpr, 
                         LNB_MPR_FILE_TYPE, 
                         COUNTDATASP, 
                         FALSE);
    prepare_list_section($pdb, 
                         $pmpr, 
                         LNB_MPR_FILE_TYPE, 
                         COUNTDATASP2, 
                         FALSE);
    prepare_list_section($pdb, 
                         $pmpr, 
                         LNB_MPR_FILE_TYPE, 
                         TRACEDATASP, 
                         FALSE);
    prepare_list_section($pdb, 
                         $pmpr, 
                         LNB_MPR_FILE_TYPE, 
                         TRACEDATASP_2, 
                         FALSE);
    prepare_list_section($pdb, 
                         $pmpr, 
                         LNB_MPR_FILE_TYPE, 
                         ISPINFODATA, 
                         FALSE);
    prepare_list_section($pdb, 
                         $pmpr, 
                         LNB_MPR_FILE_TYPE, 
                         MASKISPINFODATA, 
                         FALSE);
    #
    return;
}
#
sub load_mpr_sections
{
    my ($pmpr) = @_;
    #
    load_name_value($pmpr, INDEX());
    load_name_value($pmpr, INFORMATION());
    #
    load_list($pmpr, TIMEDATASP());
    load_list($pmpr, COUNTDATASP());
    load_list($pmpr, COUNTDATASP2());
    load_list($pmpr, TRACEDATASP());
    load_list($pmpr, TRACEDATASP_2());
    load_list($pmpr, ISPINFODATA());
    load_list($pmpr, MASKISPINFODATA());
}
#
sub prepare_mpr_files
{
    my ($pmprs, $pdb) = @_;
    #
    printf $log_fh "\n%d: Audit MPR files:\n", __LINE__;
    #
    foreach my $pmpr (@{$pmprs})
    {
        printf $log_fh "\n%d: Audit mpr: %s\n", __LINE__, $pmpr->{file_name}
            if ($verbose >= MIDVERBOSE);
        #
        next unless (load($pmpr) != 0);
        #
        load_mpr_sections($pmpr);
        #
        prepare_mpr_file($pdb, $pmpr);
    }
    #
    return;
}
#
sub process_mpr_files
{
    my ($pmprs) = @_;
    #
    # any files to process?
    #
    if (scalar(@{$pmprs}) <= 0)
    {
        printf $log_fh "\n%d: No MPR files to process. Returning.\n\n", __LINE__;
        return;
    }
    #
    my %csv_db = ();
    prepare_mpr_files($pmprs, \%csv_db);
    export_mpr_data_as_csv(\%csv_db);
    #
    return;
}
#
########################################################################
########################################################################
#
# start main execution.
#
# $mu->record("Start of script ...");
#
my %opts;
if (getopts('?MHhwWv:t:l:o:d:', \%opts) != 1)
{
    short_usage($cmd);
    exit 2;
}
#
foreach my $opt (%opts)
{
    if (($opt eq "h") or ($opt eq "?"))
    {
        short_usage($cmd);
        exit 0;
    }
    elsif ($opt eq "H")
    {
        long_usage($cmd);
        exit 0;
    }
    elsif ($opt eq "M")
    {
        $remove_mount = TRUE;
    }
    elsif ($opt eq "w")
    {
        $verbose = MINVERBOSE;
    }
    elsif ($opt eq "W")
    {
        $verbose = MIDVERBOSE;
    }
    elsif ($opt eq "v")
    {
        if ($opts{$opt} =~ m/^[0123]$/)
        {
            $verbose = $opts{$opt};
        }
        elsif (exists($verbose_levels{$opts{$opt}}))
        {
            $verbose = $verbose_levels{$opts{$opt}};
        }
        else
        {
            printf $log_fh "\n%d: Invalid verbose level: $opts{$opt}\n", __LINE__;
            short_usage($cmd);
            exit 2;
        }
    }
    elsif ($opt eq "t")
    {
        $file_type = $opts{$opt};
        $file_type =~ tr/[A-Z]/[a-z]/;
        if ($file_type !~ m/^(u01|u03|mpr)$/i)
        {
            printf $log_fh "\n%d: Invalid file type: $opts{$opt}\n", __LINE__;
            short_usage($cmd);
            exit 2;
        }
    }
    elsif ($opt eq "l")
    {
        local *FH;
        $logfile = $opts{$opt};
        open(FH, '>', $logfile) or die $!;
        $log_fh = *FH;
        printf $log_fh "\n%d: Log File: %s\n", __LINE__, $logfile;
    }
    elsif ($opt eq "o")
    {
        my $option = $opts{$opt};
        $option =~ tr/[a-z]/[A-Z]/;
        if (exists($allowed_proc_options{$option}))
        {
            $proc_options |= $allowed_proc_options{$option};
        }
        else
        {
            printf $log_fh "\n%d: Invalid option type: $opts{$opt}\n", __LINE__;
            short_usage($cmd);
            exit 2;
        }
    }
    elsif ($opt eq "d")
    {
        $export_dir = $opts{$opt};
        mkpath($export_dir) unless ( -d $export_dir );
        printf $log_fh "\n%d: Export directory: %s\n", __LINE__, $export_dir;
    }
}
#
if (scalar(@ARGV) == 0)
{
    printf $log_fh "%d: No directories given.\n", __LINE__;
    short_usage($cmd);
    exit 2;
}
# $mu->record("After getopt ...");
#
printf $log_fh "\n%d: Scan directories for U01, U03 and MPR files: \n\n", __LINE__;
#
remove_mount_fields() if ($remove_mount == TRUE);
#
my @u01_files = ();
my @u03_files = ();
my @mpr_files = ();
#
get_all_files($file_type,
             \@ARGV,
             \@u01_files,
             \@u03_files,
             \@mpr_files);
# $mu->record("After get_all_files()  ...");
#
printf $log_fh "%d: Number of U01 files: %d\n", __LINE__, scalar(@u01_files);
printf $log_fh "%d: Number of U03 files: %d\n", __LINE__, scalar(@u03_files);
printf $log_fh "%d: Number of MPR files: %d\n\n", __LINE__, scalar(@mpr_files);
#
process_u01_files(\@u01_files);
# $mu->record("After process u01 files ...");
process_u03_files(\@u03_files);
# $mu->record("After process u03 files ...");
process_mpr_files(\@mpr_files);
# $mu->record("After process mpr files ...");
#
# $mu->dump();
#
printf $log_fh "\n%d: All Done\n", __LINE__;
#

exit 0;
