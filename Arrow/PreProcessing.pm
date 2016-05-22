# >Last Modified on Fri, 20 May 2016< 
use strict;
use warnings;
use utf8;

#### 1. Arrow::LabelEncoder
#
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
#
# # Usage 
#     my $lr = Arrow::LabelEncoder->new;
#     $lr->fit({NameOfVariable => $arrayref}) # 
#     $lr->transform({NameOfVariable => $arrayref}) # convert the labels
#     $lr->inverse_transform({NameOfVariable => $arrayref}) # 
#
#
#### 2. MultiLabelBinarizer
#
# An MultiLabelBinarizer object converts class labels to a binary row vector.
# For example if there are classes 'a', 'b' and 'c', then a vector of class
# vector ['a','c','b','a'] is converted into the following 4x3 matrix:
# [ [1,0,0]   # a
#   [0,0,1]   # c
#   [0,1,0]   # b
#   [1,0,0] ] # a







package Arrow::LabelEncoder; ###################################################
use Carp;
use List::MoreUtils qw(uniq); 

########## constructor
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


########## getter and setter 
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


########## transformations
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

package Arrow::MultiLabelBinarizer; ############################################
use Carp;
use List::MoreUtils qw(uniq); 
##########
# $mlb->{fit}->{name_of_class} = ['a','b','c']
########## 

sub new {
  my $class = shift;
  bless {}, $class;
}

sub classes {
  my $self = shift;
  my $name = shift;
  return [@{$self->{fit}->{$name}}];
}

sub n_classes {
  my $self = shift;
  my $name = shift;
  return scalar(@{$self->{fit}->{$name}});
}

sub fit_or_transform {
  my $self = shift;
  my $fit = shift;
  my $transform = shift;
  croak "One of 'fit' or 'transform' must be True." if not ($fit or $transform);

  my @matrixes;
  while (@_) {
    my $name = shift; # a name of variable
    my $values = shift; # an arrayref of class label

    my @labels;
    if ($fit) {
      @labels = sort {$a cmp $b} uniq(@$values);
      $self->{fit}->{$name} = \@labels;
    } else {
      @labels = @{$self->{fit}->{$name}};
    }

    if ($transform) {
       my %converter = map {$labels[$_]=>$_} 0..$#labels; # a=>0, b=>1, ...

      my @matrix;
      foreach my $y (@$values) {
	croak "The label '$y' is invalid." if (not defined $converter{$y});
	my @row = (0) x @labels;
	$row[$converter{$y}] = 1;
	push(@matrix,\@row);
      }
      push(@matrixes,\@matrix);
    }
  }

  if ($transform) {
    return @matrixes == 1 ? $matrixes[0] : \@matrixes;
  } else {
    return $self;
  }
}

sub fit {
  my $self = shift;
  return $self->fit_or_transform(1,0,@_);
}

sub transform {
  my $self = shift;
  return $self->fit_or_transform(0,1,@_);
}

sub fit_transform {
  my $self = shift;
  return $self->fit_or_transform(1,1,@_);
}

sub inverse_transform{
  my $self = shift;
  my @results;
  while (@_) {
    my $name = shift;
    my $arrayref = shift;                        # [1,0,2,0,...]
    my @labels = @{$self->{fit}->{$name}};       # ['a','b','c']
    my @result = map { $labels[$_] } @$arrayref; # ['b','a','c','a',...] 
    push(@results,\@result);
  }
  return @results == 1 ? $results[0] : \@results;
}


1;
