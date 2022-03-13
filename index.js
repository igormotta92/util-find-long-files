
// Foi necessário utilizar o comando abaixo para rodar um .csv > 1.000.000 de linhas.
// node --max-old-space-size=8192 index.js

const fs = require('fs');
const csv = require('fast-csv');

const aba = 'aba12'
const stream = fs.createReadStream(`./planilha/planilha_${aba}.csv`);

if (fs.existsSync(`./planilha/geradas/${aba}_so_duplicado_all.csv`)) fs.unlinkSync(`./planilha/geradas/${aba}_so_duplicado_all.csv`);
if (fs.existsSync(`./planilha/geradas/${aba}_sem_duplicata_all.csv`)) fs.unlinkSync(`./planilha/geradas/${aba}_sem_duplicata_all.csv`);

let headers_txt = '';
write(`${aba}_so_duplicado_all`, headers_txt);
//write(`${aba}_sem_duplicata_all`, headers_txt);

//const numeros_pedidos = [];
let duplicados = 1;
const banco = new Map();

let count_row = 2;
const streamCsv = csv.parse({
  headers: true,
  delimiter: ',',
  ignoreEmpty: true,
  // maxRows: 10000,
})
  .on('data', (data) => {
    if (count_row % 10000 == 0) {
      console.log(`Linha: ${count_row}`);
    }
    count_row++;

    if (banco.has(data['Número do Pedido'])) {
      duplicados++;

      let { count, row } = banco.get(data['Número do Pedido']);
      count++;

      //só pode entrar uma vez, vai escrever o primeiro valor que é lido
      let txt;
      if (count === 2) {
        banco.set(data['Número do Pedido'], { count, row: {} });
        txt = Object.values(row).join(',')
        write(`${aba}_so_duplicado_all`, txt);
      } else {
        banco.set(data['Número do Pedido'], { count, row });
      }

      txt = Object.values(data).join(',')
      write(`${aba}_so_duplicado_all`, txt);

    } else {
      banco.set(data['Número do Pedido'], { count: 1, row: data });
    }

    //console.log(data)
    /*if (numeros_pedidos.includes(data['Número do Pedido'])) {
      duplicados.push(data);

      let txt = Object.values(data).join(',')
      write(`${aba}_so_duplicado`, txt);

    } else {
      numeros_pedidos.push(data['Número do Pedido']);

      let txt = Object.values(data).join(',')
      write(`${aba}_sem_duplicata`, txt);
    }*/

  })
  .on('end', () => {
    //console.log(duplicados);
    console.log('Total duplicados: ' + duplicados);
  })

stream.pipe(streamCsv);

function write(file, txt) {
  try {
    fs.appendFileSync(`./planilha/geradas/${file}.csv`, txt + '\r\n', 'utf8');
    //console.log('The "data to append" was appended to file!');
  } catch (err) {
    console.log('error: write file');
  }

}