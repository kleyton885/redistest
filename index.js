const { Pool } = require('pg');

let pg_conexao = null;
let inserindo_registros = false;

async function getPgConexao(){
  const pool = new Pool({
    connectionString: 'pg://postgres:postgres@localhost:5432/postgres'
  });

  if (pg_conexao == null){
    pg_conexao = await pool.connect();
  }

  return pg_conexao;
}

async function preparaTabela(num_registros){
  await pg_conexao.query(`CREATE TABLE IF NOT EXISTS clientes (
    nome text,
    idade numeric
  )`);

  const { rows } = await pg_conexao.query(`select count(*) from clientes`);

  if (rows[0].count < num_registros){
    inserindo_registros = true;
    quantidade_para_inserir = num_registros - rows[0].count;
    console.log("Inserindo novos " + quantidade_para_inserir + " registros..")
    for(let i=0; i < quantidade_para_inserir; i++){
      await pg_conexao.query(`INSERT INTO clientes(nome,idade) VALUES ('nome-${i}',${i});`);
    }
    console.log("OK\n")
    inserindo_registros = false;
  }
}

async function testaSemRedis(num_registros){
  console.time("semredis");
  await pg_conexao.query(`select * from clientes LIMIT ${num_registros}`);
  console.timeEnd("semredis");
}

async function testaComRedis(num_registros){
  const redis = require('promise-redis')();
  const redis_client = redis.createClient();

  redis_client.on("error", (error) => {
    console.error(error);
  });

  await redis_client.del('listaClientes');
  const { rows } = await pg_conexao.query(`select * from clientes LIMIT ${num_registros}`);
  await redis_client.set('listaClientes', JSON.stringify(rows));
  await redis_client.expire('listaClientes', 30);

  console.time("comredis");
  const listaClientes = await redis_client.get('listaClientes');
  console.timeEnd("comredis");
  redis_client.quit();
}

(async () => {
  try {
    console.log("Abrindo conexão com o postgres..");
    pg_conexao = await getPgConexao();
    console.log("OK\n");

    console.log("Preparando a tabela de clientes para teste..");
    const prompt = require('prompt-sync')();
    const num_registros = prompt('Informe a quantidade de registros: ');
    await preparaTabela(num_registros);

    console.log("Calculando o tempo de resposta do select de " + num_registros + " registros..");
    testaSemRedis(num_registros);
    testaComRedis(num_registros);
  } catch (e){
    console.log(e.message)
  } finally {
    const checkdb = setInterval(() => {
      if (!inserindo_registros){
        setTimeout(() => {
          if (pg_conexao != null){
            pg_conexao.end();
            clearInterval(checkdb);
          }
        }, 2000)
      }
    }, 500);
  }
})();