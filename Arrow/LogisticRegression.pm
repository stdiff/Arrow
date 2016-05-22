package Arrow::LogisticRegression;
# >Last Modified on Sat, 21 May 2016< 
use strict;
use warnings;
use utf8;
use Carp;
use PDL;
use Arrow::PreProcessing;

# This package provides a penalised logistic regression classifier.

#################################################################### Constructor
sub new {
  my $class = shift;
  my %pref = @_;

  my %tuning; # tuning parameters
  $tuning{lambda} = $pref{lambda} || 0 ;
  $tuning{iter} = $pref{iter} || 0 ;
  $tuning{class_weight} = $pref{class_weight};

  my %fit; # fitted data
  $fit{beta} = []; # estimated linear coefficients (PDL)
  $fit{beta_seq} = []; 
  $fit{intercept_} = [];
  $fit{coef_} = [];
  $fit{costs} = [];
  $fit{mlb} = Arrow::MultiLabelBinarizer->new;

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
  use Arrow::Optimizer qw(newton_raphson);

  my $self = shift;
  return $self->{fit} if @_ == 0; # give the result of fitting if no arg.

  my $X_orig = shift;
  my $y_orig = shift;
  my ($p, $n) = dims($X_orig);

  # if $y is a piddle, then it is converted into an arrayref
  my $yclass = ref $y_orig eq 'PDL' ? [$y_orig->list] : $y_orig;
  my $ybinary = pdl $self->{fit}->{mlb}->fit_transform(target=>$yclass);
  my $K = $ybinary->dim(0); # the number of classes

  croak "only one class..." if $K == 1;

  my $sample_weight = pdl [(1) x $n];
  if ($self->{tuning}->{class_weight}) {
    my %class_weight = %{$self->{tuning}->{class_weight}};
    $sample_weight = pdl [map { $class_weight{$_} } @$yclass];
  }
  $sample_weight = $sample_weight->transpose;

  my $lambda = $self->{tuning}->{lambda};
  my $lambda_mat = stretcher pdl (0,($lambda)x$p); # diag(0,lambda,...,lambda)

  my $X = ones(1,$n)->glue(0,$X_orig);
  my $Xt = ($X * $sample_weight)->transpose;
  ### WARNING! $Xt is not just the transpose of $X. $Xt includes sample weights!

  my $hessian = sub {
    my $beta = shift;
    my $p = 1/(1+exp(-$X x $beta));
    $p = $p->flat;
    my $W = stretcher ($p*(1-$p));
    
    my $hessian = $Xt x $W x $X;
    $hessian += $lambda_mat if ($lambda);
    return $hessian;
  };

  foreach my $k (0..($K-1)) {
    my $y = $ybinary->slice("$k,:"); # the target variable (k-th class)

    my $gradient = sub { ## this function require the target values
      my $beta = shift;
      my $p = 1/(1+exp(-$X x $beta));
      my $grad_l = ($Xt) x ($y-$p);

      if ($lambda) {
	return -$grad_l + $lambda_mat x $beta;
      } else {
	return -$grad_l;
      }
    };

    my $cost = sub {
      my $beta = shift;
      my $Xbeta = $X x $beta;
      my $lambda = $self->{tuning}->{lambda};
      my $penalty = ($lambda/2)*(sum($beta**2)-$beta->at(0,0)**2);
      return -sum($y*$Xbeta - log(1+exp($Xbeta))) + $penalty;
    };

    my $init = zeros(1,$p+1);
    my ($beta,$costs,$betas) = newton_raphson($hessian,$gradient,$init,cost=>$cost);

    my @coef = $beta->list;
    push(@{$self->{fit}->{beta}},[@coef]);
    push(@{$self->{fit}->{intercept_}},shift(@coef));
    push(@{$self->{fit}->{coef_}},\@coef);
    push(@{$self->{fit}->{costs}},$costs);
    push(@{$self->{fit}->{beta_seq}},[@$betas]);
  }

  foreach my $prop ('beta','intercept_','coef_') {
    $self->{fit}->{$prop} = pdl $self->{fit}->{$prop};
  }
  # intercept0, beta0, beta1, ..., beta p
  # intercept1, beta0, beta1, ..., beta p
  # ....
  # interceptK
  # Thus we take its transpose.
  $self->{fit}->{coef_} = $self->{fit}->{coef_}->transpose;
  $self->{fit}->{beta} = $self->{fit}->{beta}->transpose;

  return $self;
}

sub cost_function {
  my $self = shift;
  my $X = shift;
  my $y = shift; # this assumes that $y is a 0/1 vector. 
  $y = (pdl $y)->transpose;
  my $beta = shift || $self->{fit}->{beta};
  my $coef = $beta->slice(':,1:');
  my $intercept = $beta->slice(':,0');
  my $Xbeta = $intercept + $X x $coef;
  return -sumover( ($y*$Xbeta - log(1+exp($Xbeta)))->transpose );# + $penalty;
}

sub coef_ {
  my $self = shift;
  return $self->{fit}->{coef_};
}

sub intercept_ {
  my $self = shift;
  return $self->{fit}->{intercept_}
}

sub beta_seq {
  my $self = shift;
  return $self->{fit}->{beta_seq};
}

sub predict {
  my $self = shift;
  my $X = shift;

  my $proba = $self->predict_proba($X);
  my @class_number = $proba->maximum_ind->list;
  return $self->{fit}->{mlb}->inverse_transform('target'=>\@class_number);
}

sub predict_proba {
  my $self = shift;
  my $X = shift; # piddle
  my $K = $self->{fit}->{mlb}->n_classes('target'); # number of classes

  my $beta = $self->{fit}->{beta};
  my $intercepts = $beta->slice(':,0');
  $beta = $beta->slice(':,1:');

  my $z = $intercepts + $X x $beta;
  my $T = 1/(1+exp(-$z));

  # when we make a multinomial classifier as a one-versus-rest classifier,
  # we create a classifier for every class and estimate the probabilities
  # with all classifiers. For an observation x, we obtain a vector of 
  # probabilities: (p1(x),p2(x),...,pK(x)). But the sum of probabilities
  # is not 1 in general. So we have to normalise the vector, so that the 
  # sum of components is 1. We hire the easiest way to do that: let s be
  # the sum of the components, then (p1(x)/s,...,pK(x)/s) is what we want.

  my $sum = sumover $T; # sum along x-coord (but 1x$n matrix).
  return $T / $sum->transpose();
}

sub classes {
  my $self = shift;
  return $self->{fit}->{mlb}->classes('target');
}

1;
