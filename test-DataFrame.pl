#!/usr/bin/perl 
# >Last Modified on Thu, 17 Dec 2015< 
use strict;
use warnings;
use utf8;
use v5.10;
use LWP::Simple qw(getstore);
use LWP::UserAgent;
use Data::Dumper;
use Arrow::DataFrame;
binmode(STDOUT, ":utf8");

### The CSV file in Internet for test 
my $url = 'http://www-bcf.usc.edu/~gareth/ISL/Auto.csv';
### The file name of the CSV.
my $csv_file = "Auto.csv";

### download the CSV file for test
if (!-R $csv_file) {
  my $response_code = getstore($url,$csv_file);
  if ($response_code != 200) {
    print "The download has failed.";
    print "URL: $url\n";
    print "HTTP Response code: $response_code\n;";
    unlink($csv_file) if -e $csv_file;
    die "The download has failed. ";
  } 
}

say "Welcome to the test script for DataFrame.pm!";

say 'Import the CSV file Auto.csv: ';
say 'my $df = Arrow::DataFrame->read_csv($csv_file);';
my $df = Arrow::DataFrame->read_csv($csv_file);
pause();

say 'Look at the first three rows: $df->head(3)'; 
$df->head(3);
pause();

say "Change temporary the width of each cell to 10 chars and show the first three rows again.";
say '$df->show(head=>3,width=>10)';
$df->show(head=>3,width=>10);
pause();


say "names() gives the arrayref of names of columns.";
show_arrayref($df->names);
say "Keep in mind that we hava a few rules for the column names.";
say "Please read the section DESCRIPTION in the documentation.";
pause();


say 'set_name() change a column name: $df->set_name(\'cyl\',1); ...';
my @names = @{$df->names};
@names[1,2,3,5] = qw(cyl disp pow acc);
$df->set_name('cyl',1);
$df->set_name('disp',2);
$df->set_name('pow',3);
$df->set_name('acc',5);
pause();


say 'See the last three rows: $df->tail(3)';
$df->tail(3);
say 'The column names has been changed.';
pause();


say "Check the size of the data frame.";
say "nrow(): ", $df->nrow, " rows";
say "ncol(): ", $df->ncol, " columns";
pause();


say 'See the 2nd column: $df->cols(1)';
show_arrayref($df->cols(1));
pause();


say 'Make the 101st row a hash: $df->rows_hash(100)';
print Dumper($df->rows_hash(100));
pause();


say 'Remove a column: my $arrayref = $df->del_cols("name")';
my $arrayref = $df->del_cols("name");
say 'The return value of the method is the removed column.';
say 'Check the last 3 rows again.';
$df->tail(3);
pause();


say 'Add a column to a data frame: $df = $df->cbind(name=>$arrayref)';
$df = $df->cbind(name=>$arrayref);
say 'Check the first 3 rows again';
$df->head(3);
pause();


say 'Apply an aggregate function to a column: my $ave = $df->capply(\&mean,"weight")';
say 'output: ', $df->capply(\&mean,"weight");
say 'Note that mean() should be implemented by yourself.';
pause();


say 'Some functions which are similar to ones of dplyr are available.';
say '$df->filter(sub { grep { $_ eq \'?\' } @_ })->show;';
$df->filter(sub { grep { $_ eq '?' } @_ })->show;
say 'Here "?" is an NA character of the CSV.';
pause();


say 'A method with "_th" accepts a function reference which take a hashref.';
say '$df->filter_th(sub { $_[0]{"name"} =~ /honda/ })->show;';
$df->filter_th(sub { $_[0]{"name"} =~ /honda/ })->show;
pause();


say 'Sort by "mpg" and "disp"';
say '$df->arrange(\'mpg\',\'disp\')->head(9);';
$df->arrange('mpg','disp')->head(9);
pause();


say '$df->distinct("cyl","origin")->show;';
$df->distinct("cyl","origin")->show;
pause();


say 'my @origin = qw(usa europa japan);';
say '$df->mutate_th(\'origin\', sub { $origin[$_[0]{origin}-1] } )->show(from=>10,to=>20);';
my @origin = qw(usa europa japan);
$df->mutate_th('origin', sub { $origin[$_[0]{origin}-1] } )->show(from=>10,to=>20);
pause();

say "I strongly recommend reading the section DESCRIPTION in the documentation.";
say "Try the following command.";
say "\$ perldoc Arrow/DataFrame.pm";

say "Thank you for trying DataFrame.pm!";

exit;
#---------------------------------------------------------------------------
sub pause {
  print "\nHit the enter to continue: ";
  <STDIN>;
  print "\n",qw(-)x50,"\n\n";
}

sub show_arrayref {
  my $arrayref = shift;
  print "[ ", (join ", ", @$arrayref), " ]\n";
}

sub mean {
  my $sum = 0;
  $sum += $_ foreach @_;
  return $sum/@_;
}


