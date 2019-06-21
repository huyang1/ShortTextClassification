package examples.liblinear

/**
 * SolverType defines the names of different solvers.
 */
object SolverType extends Enumeration
{
	type Solver = Value
	val L2_LR = Value(0)
	val L2_L2LOSS_SVC = Value(2)
	val FTRL = Value(3)

	val unknown = Value(-1)

	def parse(id : Int) : Value =
	{
		if(id == L2_LR.id)
		{
			return L2_LR;
		}
		else if(id == L2_L2LOSS_SVC.id)
		{
			return L2_L2LOSS_SVC
		}
		else if(id == FTRL.id)
		{
				return FTRL
		}
		else
		{
			return unknown
		}
	}
}

/**
 * Parameter stores the type of solver and user-specified parameters.
 *
 * @param solverType the optimization solver
 * @param eps used in stopping criteria
 * @param C used to control the regularization and loss.
 */
class Parameter() extends Serializable
{
	var solverType : SolverType.Value = SolverType.L2_LR
	var eps : Double = 1e-2
	var C : Double = 1.0
	var numSlaves = -1
}
