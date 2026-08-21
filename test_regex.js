const json = `
{
  "t2": [
      1519485756735754331,
      1431565435819528302
  ],
  "foo": 1234567890123456789,
  "bar": "1234567890123456789"
}
`;
const safeJson = json.replace(/(?<!["\w])\b\d{16,20}\b(?!["\w])/g, '"$&"');
console.log(safeJson);
