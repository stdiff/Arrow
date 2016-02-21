package Arrow::LinearRegression;
# >Last Modified on Sun, 21 Feb 2016< 
use strict;
use warnings;
use utf8;
use Carp;
use PDL;

# This package provides a (ridge) linear regression classifier.

#################################################################### Constructor
sub new {
  my $class = shift;
  my %pref = @_;

  my %tuning; # tuning parameters
  $tuning{lambda} = $pref{lambda} || 0 ;
  $tuning{method} = $pref{lambda} || 'normal' ;
  # for gradient decent
  # $tuning{alpha} = $pref{alpha} || 0.01 ;
  # $tuning{iter} = $pref{iter} || 0 ;

  my %fit; # fitted data
  $fit{beta} = undef; # estimated linear coefficients (PDL)
  $fit{intercept} = undef;
  $fit{coef} = undef;
  $fit{mse} = undef;  # mean squared error
  $fit{score} = undef;  # score (R^2)

  return bless {tuning=>\%tuning,fit=>\%fit}, $class;
}

######################################################################### Getter
sub tuning{
  my $self = shift;
  return $self->{tuning};
}

######################################################################## methods
sub fit{
  use PDL::Slatec qw(matinv);
  use PDL::NiceSlice;

  my $self = shift;
  return $self->{fit} if @_ == 0; # give the result of fitting if no arg.

  my $X = shift;
  my $y = shift;
  my ($p, $n) = dims($X);

  # Adding 1s on the left of the original matrix X
  my $X0 = ones(1,$n);
  $X = $X0->append($X);
  my $Xt = $X->transpose;

  # Estimating the parameter beta (with normal equation).
  my $beta;
  my $lambda = $self->{tuning}->{lambda};
  if ($lambda) {
    my $I = identity($p+1);
    $I(0,0) .= 0;
    $beta =  matinv($Xt x $X + $lambda*$I) x $Xt x $y;
  } else {
    $beta =  matinv($Xt x $X) x $Xt x $y;
  }
  $self->{fit}->{beta} = $beta;

  # intercept and coef
  my @coef = $beta->list;
  $self->{fit}->{intercept} = shift @coef;
  $self->{fit}->{coef} = \@coef;

  # Predict the value of the target varibable on the training set.
  my $yhat = $self->{fit}->{yhat} = $X x $beta;

  # RSS
  my $rss = sum(($y-$yhat)**2);

  # root of the mean squared error (RMSE)
  $self->{fit}->{rmse} = sqrt($rss/$n);

  # score (R^2 coefficient)
  $self->{fit}->{score} = 1 - $rss/sum(($y-avg($y))**2);

  # cost (RSS+penalty)
  my $penalty = 0;
  if ($lambda) {
    $penalty += $_**2 foreach (@coef);
    $penalty *= $lambda;
  }
  $self->{fit}->{cost} = $rss + $penalty;

  return $self->{fit};
}

sub predict{
  my $self = shift;
  my $X = shift;
  my $beta = $self->{fit}->{beta};
  croak "We have not fit any model." if not defined $beta;

  my ($p, $n) = dims($X);  

  my $X0 = ones(1,$n);
  my $Xtot = $X0->append($X);

  return $Xtot x $beta;
}

sub score{
  # $estimator->score($X,$y_true,)
  my $self = shift;
  my $X = shift;
  my $y_true = shift;
  my $type = shift || 'R2';
  croak 'Usage: $estimator->score($X,$y_true[,"rmse"])' if not defined $y_true;

  my $yhat = $self->predict($X);

  if ($type eq 'mean_squared_error') {
    # Mean Squared Error
    return avg(($y_true-$yhat)**2);
  } elsif ($type eq 'neg_mse') {
    # -(Mean Squared Error)
    return -avg(($y_true-$yhat)**2);
  } else {
    # R2 
    my $u = sum(($y_true-$yhat)**2);
    my $v = sum(($y_true-avg($y_true))**2);
    return 1-$u/$v;
  }
}

1;
