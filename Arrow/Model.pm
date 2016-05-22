package Arrow::Model;
# >Last Modified on Sun, 22 May 2016< 
use strict;
use warnings;
use utf8;
use Carp;

use Exporter qw(import);
our @EXPORT = qw(LinearRegression LogisticRegression);

sub LinearRegression {
  use Arrow::LinearRegression;
  return Arrow::LinearRegression->new(@_);
}

sub LogisticRegression {
  use Arrow::LogisticRegression;
  return Arrow::LogisticRegression->new(@_);
}

1;
