package Arrow::LabelEncoder;
# >Last Modified on Fri, 12 Feb 2016< 
use strict;
use warnings;
use utf8;
use Carp;
use List::MoreUtils qw(uniq); 

#use Arrow::DataFrame;

# An LabelEncoder object is used to convert class labels to integers and 
# vice versa. This is important if we do mathematics for a data frame 
# containing a class variable. 
# 
# bless { var0 => ['a','b','c'], var1 => [1,-1], ... }, Arrow::LabelEncoder
# 
# This class is inspired by LabelEncoder of scikit-learn. But there is a 
# difference: One object can manage several class variables. (In scikit-learn
# a class variable corresponds to an LabelEncoder object one-to-one.)
# Because of this we need to specify the class variable(s) when we convert 
# the labels.

#################################################################### constructor
sub new {
  my $class = shift;
  return bless {}, $class;
}

sub fit {
  # fit($hashref)
  my $class = shift;
  my $hashref = shift;
  my $le = Arrow::LabelEncoder->new;

  return $le->add($hashref);
}


############################################################## getter and setter 
sub variables {
  # The array of variables. 
  my $self = shift;
  return sort keys %$self;
};

sub labels {
  # labels('col0');
  # Show the arrayref of the labels of specified variable
  my $self = shift;
  my $var = shift;
  return defined $self->{$var} ? $self->{$var} : [];

}

sub add {
  # add($hashref)
  my $self = shift;
  my $hashref = shift;

  foreach my $var (keys %$hashref) {
    if (defined $self->{$var}) {
      # The order of the arguments of uniq() is important.
      $self->{$var} = [uniq (@{$self->{$var}},@{$hashref->{$var}})];
    } else {
      $self->{$var} = [uniq (@{$hashref->{$var}})];
    }
  }
  return $self;
}


################################################################ transformations
sub transform {
  # converts class labels to integers. 
  # transform($hashref);
  my $self = shift;
  my $hashref = shift;

  my %results;
  foreach my $var (keys %$hashref) {
    croak "No data for the variable $var." if not defined $self->{$var};
    my %dict = map { $self->{$var}->[$_] => $_ } 0..$#{$self->{$var}};
    $results{$var} = [map { $dict{$_} } @{$hashref->{$var}}];
  }
  return \%results;
}

sub inverse_transform {
  # recover the labels 
  # inverse_transform($hashref);
  my $self = shift;
  my $hashref = shift;

  my %results;
  foreach my $var (keys %$hashref) {
    croak "No data for the variable $var." if not defined $self->{$var};
    $results{$var} = [@{$self->{$var}} [@{$hashref->{$var}}]];
  }
  return \%results;
}


1;
