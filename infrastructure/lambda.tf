resource "aws_lambda_function" "executa_emr" {
  # biblioteca e modulos que precisam para o codigo lambdar funcionar, aqui ficar todos as biblitecas que vamos precisar, o arquuivo será criado na esteira do git
  filename      = "lambda_function_payload.zip"
  function_name = var.lambda_function_name
  role          = aws_iam_role.lambda.arn
  handler       = "lambda_function.handler"
  #tamanho de memoria
  memory_size = 128
  timeout     = 30

  # Comando para fazer o controle de estato
  source_code_hash = filebase64sha256("lambda_function_payload.zip")

  # Qual linguagem vamos utilizar
  runtime = "python3.8"

  tags = {
    IES   = "IGTI"
    CURSO = "EDC"
  }

}