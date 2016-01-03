#!/usr/bin/perl -w
######################################################################
#
# process NPM/LNB u01, u03 and mpr files. write out delta data and raw
# data to CSV files.
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
######################################################################
#
# globals
#
my $cmd = $0;
my $log_fh = *STDOUT;
#
# cmd line options
#
my $logfile = '';
my $verbose = NOVERBOSE;
my $file_type = "all";
my $export_dir = '/tmp/';
#
my %verbose_levels =
(
    off => NOVERBOSE(),
    min => MINVERBOSE(),
    mid => MIDVERBOSE(),
    max => MAXVERBOSE()
);
#
########################################################################
#
# miscellaneous functions
#
sub usage
{
    my ($arg0) = @_;
    print $log_fh <<EOF;

usage: $arg0 [-?] [-h] \\ 
        [-w | -W |-v level] \\ 
        [-t u10|u03|mpr] \\ 
        [-l logfile] \\ 
        [-d path] \\
        directory ...

where:
    -? or -h - print this usage.
    -w - enable warning (level=min=1)
    -W - enable warning and trace (level=mid=2)
    -v - verbose level: 0=off,1=min,2=mid,3=max
    -t file-type = type of file to process: u01, u03, mpr.
                   default is all files.
    -l logfile - log file path
    -d path - export directory, defaults to '/tmp'.

EOF
}
#
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
########################################################################
#
sub process_files
{
    my ($pfiles, $ftype) = @_;
    #
    # any files to process?
    #
    if (scalar(@{$pfiles}) <= 0)
    {
        printf $log_fh "\n%d: No %s files to process. Returning.\n\n", __LINE__, $ftype;
        return;
    }
    #
    printf $log_fh "\n%d: Processing %s files:\n", __LINE__, $ftype;
    printf $log_fh "%d: Number of %s files: %d\n", __LINE__, $ftype, scalar(@{$pfiles});
    #
    foreach my $pfile (@{$pfiles})
    {
        printf $log_fh "\n%d: Process %s: %s\n", __LINE__, $ftype, $pfile->{file_name}
            if ($verbose >= MIDVERBOSE);
    }
    #
    return;
}
#
########################################################################
#
# start main execution.
#
my %opts;
if (getopts('?hwWv:t:l:d:', \%opts) != 1)
{
    usage($cmd);
    exit 2;
}
#
foreach my $opt (%opts)
{
    if (($opt eq "h") or ($opt eq "?"))
    {
        usage($cmd);
        exit 0;
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
            usage($cmd);
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
            usage($cmd);
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
    usage($cmd);
    exit 2;
}
#
printf $log_fh "\n%d: Scan directories for U01, U03 and MPR files: \n\n", __LINE__;
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
#
process_files(\@u01_files, LNB_U01_FILE_TYPE);
#
process_files(\@u03_files, LNB_U03_FILE_TYPE);
#
process_files(\@mpr_files, LNB_MPR_FILE_TYPE);
#
printf $log_fh "\n%d: All Done\n", __LINE__;
#
exit 0;
