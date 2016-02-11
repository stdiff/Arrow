package Arrow::GroupBy;
# >Last Modified on Thu, 11 Feb 2016< 
use strict;
use warnings;
use utf8;
use Carp;
use List::MoreUtils qw(uniq);

# An object of GroupBy consists of two data:
# 1. levels (arrayref)
#    An arrayref of names of grouped columns. 
# 2. data_frame (Arrow::DataFrame)
#    An DataFrame object which contains 
# 
# An GroupBy object stores all the data on a memory (instead of keeping
# or calculating the rows on demand). 

#################################################################### constructor
sub new {
  # new(levels=>[col0,col1],data=>[[val00,val01,df0],[val10,val11,df1],...]);
  my $class = shift; # 'GroupBy'
  my %pref = @_;
  my $df = Arrow::DataFrame->new(names=>[@{$pref{levels}},'data_frame'],rows=>$pref{data});
  return bless {levels=>$pref{levels},data=>$df}, $class;
}


######################################################################## getters
sub levels { 
  return $_[0]->{levels};
}


################################################## read the data as a data frame
sub level_values {
  # Produce the data frame consisting only of the level columns.
  my $self = shift;
  return $self->{data}->select(@{$self->levels});
}


sub group_df {
  # $grp->groupdf($i);
  # Produce a single data frame of the $i-th group. 
  # This data frame includes the level columns
  my $self = shift;
  my $i = shift; # the row (group) number to pick up
  my $levels = $self->levels;
  my $k = $self->{data}->ncol()-1; # column number of 'data_frame'

  my $left = $self->{data}->select(@$levels)->slice($i);
  my $right = $self->{data}->at([$i,$k])->[0];
  my $ni = $right->nrow; # number of rows of data frame in the group
  $left = $left->rbind($left->slice(0)) foreach (1..($ni-1));

  return $left->cbind($right);
}


sub cat {
  # Create a single data frame by concatinating the data frames
  # Namely the inverse of group_by() (up to the order of columns)
  my $self = shift;
  my $gn = $self->{data}->nrow()-1; # number of groups
  my $k = $self->{data}->ncol()-1; # column number of 'data_frame'
  my $levels = $self->levels;

  my $df;
  foreach my $i (0..$gn) {
    my $group_df = $self->group_df($i); # the data frame of the $i-th group
    $df = $i ? $df->rbind($group_df) : $group_df;
  }
  return $df;
}


######################################################### tweak a GroupBy object



########################## applying functions of dplyer to each group data frame
sub dapply {
  # $grpd->dapply(sub {$_[0]->select('mpg')});
  # Applying a function to each group data frame (without level columns),
  # The function must send a data frame to a single data frame.
  # Its return value is an object of Arrow::GroupBy
  my $self = shift;
  my $func = shift;
  my $results = $self->{data}->rapply_th(sub {$func->($_[0]{'data_frame'})});

  my $data = $self->level_values;
  $data = $data->cbind('data_frame'=>$results);
  $data = $data->filter_th(sub { $_[0]{'data_frame'}->nrow > 0 });

  return Arrow::GroupBy->new(levels=>$self->levels,data=>$data->{rows});
}


sub filter {
  my $self = shift;
  my @argm = @_;
  return $self->dapply(sub{ $_[0]->filter(@argm) });
}


sub filter_th {
  my $self = shift;
  my @argm = @_;
  return $self->dapply(sub{ $_[0]->filter_th(@argm) });
}


sub slice {
  my $self = shift;
  my @argm = @_;
  return $self->dapply(sub{ $_[0]->slice(@argm) });
}


sub arrange {
  my $self = shift;
  my @argm = @_;
  return $self->dapply(sub{ $_[0]->arrange(@argm) });
}


sub select {
  my $self = shift;
  my @argm = @_;
  return $self->dapply(sub{ $_[0]->select(@argm) });
}


sub rename {
  my $self = shift;
  my @argm = @_;
  return $self->dapply(sub{ $_[0]->rename(@argm) });
}


sub distinct {
  my $self = shift;
  my @argm = @_;
  return $self->dapply(sub{ $_[0]->distinct(@argm) });
}


sub mutate {
  my $self = shift;
  my @argm = @_;
  return $self->dapply(sub{ $_[0]->mutate(@argm) });
}


sub mutate_th {
  my $self = shift;
  my @argm = @_;
  return $self->dapply(sub{ $_[0]->mutate_th(@argm) });
}


sub summarise {
  my $self = shift;
  my @aggregations = @_;
  my $k = $self->{data}->ncol()-1; # col number of the 'data_frame'.

  my $results = $self->{data}->rapply(sub {$_[$k]->summarise(@aggregations)});
  my $aggregated = shift @$results;
  $aggregated = $aggregated->rbind($_) foreach (@$results);

  return $self->{data}->select(@{$self->levels})->cbind($aggregated);
}


sub summarize {
  my $self = shift;
  return $self->summarise(@_);
}


1;
