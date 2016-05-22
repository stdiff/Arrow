package Arrow::Optimizer;
# >Last Modified on Sun, 22 May 2016< 
use strict;
use warnings;
use utf8;
use Carp;
use PDL;

use Exporter qw(import);
our @EXPORT = qw(gradient_descent newton_raphson);

sub gradient_descent {
  ## gradient_descent($gradient,$init,cost=>$cost,lr=>$lr,momentum=>$momentum,tolerance=>$tolerance,iter=$iter)
  my $gradient = shift; # function calculating a gradient (column) vector
  my $init = shift; # an initial value of parameters

  my %pref = @_;
  my $cost = $pref{cost}; # cost function
  my $lr = $pref{lr} || 0.01; # learning rate
  my $tolerance = $pref{torelance} || 0.0001;
  my $iter = $pref{iter} || 100; # limit of itration

  my $beta = $init->copy;
  my @betas = ($beta);
  my @costs = $cost ? ($cost->($beta)) : ();

  foreach my $t (1..$iter) {
    my $grad = $gradient->($beta);
    my $beta_new = $beta -  $lr * $grad;
    push(@betas,$beta_new);
    push(@costs,$cost->($beta_new)) if $cost;
    last if ( sum($grad**2) < $tolerance );
    $beta = $beta_new->copy;
  }

  if ($cost) {
    return $beta,\@costs,\@betas;
  } else {
    return $beta,\@betas;
  }
  
}

sub newton_raphson {
  use PDL::Slatec qw(matinv);

  ## newton_rapson($hessian,$gradient,$init,cost=>$cost,tol=>0.0001)
  my $hessian = shift;  # a function calculating a hessian
  my $gradient = shift; # a function calculating a gradient (column) vector
  croak 'Give both a Hessian and a gradient.' if not ($hessian and $gradient);
  my $beta = shift; # an initial value of parameters


  if (ref $beta eq 'PDL') {
    $beta = $beta->copy;
  } else {
    croak 'Set an initial value as a piddle.';
  }

  my %param = @_;
  my $cost = $param{cost};         # cost function
  my $tol = $param{tol} || 0.0001; # torelance

  my @costs = $cost ? ($cost->($beta)) : ();
  my @betas = ($beta->copy);
  my $update = $tol + 1;
  
  while ($update > $tol) {
    my $my_hessian = $hessian->($beta);
    my $beta_new = $beta - matinv($hessian->($beta)) x $gradient->($beta);
    $update = sum(($beta_new-$beta)**2);
    $beta = $beta_new->copy;
    push(@costs,$cost->($beta)) if $cost;
    push(@betas,$beta->copy);
  }

  if ($cost) {
    return $beta,\@costs,\@betas;
  } else {
    return $beta,\@betas;
  }
}

1;
