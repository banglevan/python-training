function calculate() {
    const num1 = document.getElementById('num1').value;
    const num2 = document.getElementById('num2').value;
    const operation = document.getElementById('operation').value;
    
    fetch('/calculate', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/x-www-form-urlencoded',
        },
        body: `num1=${num1}&num2=${num2}&operation=${operation}`
    })
    .then(response => response.text())
    .then(result => {
        document.getElementById('result').textContent = result;
    })
    .catch(error => {
        document.getElementById('result').textContent = 'Error: ' + error;
    });
}