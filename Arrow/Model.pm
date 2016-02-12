package Arrow::Model;
# >Last Modified on Fri, 12 Feb 2016< 
use strict;
use warnings;
use utf8;
use Carp;

use Exporter qw(import);
our @EXPORT = qw(LinearRegression);

sub LinearRegression {
  use Arrow::LinearRegression;
  Arrow::LinearRegression->new(@_);
}

1;
