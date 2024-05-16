const button_start = document.querySelector(".run-button");
const button_end = document.querySelector(".stop-button");

async function eventClickStart() {
        // Send a request to your Flask server when the button is clicked
        const response = await fetch("/button_click", {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(jsonData)
        });
        const data = await response.json();
        console.log(data);
}

async function eventClickEnd() {
        // Send a request to your Flask server when the button is clicked
        const response = await fetch("/button_end", {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(jsonData)
        });
        const data = await response.json();
        console.log(data);
}

button_start.addEventListener("click",eventClickStart);
button_end.addEventListener("click",eventClickEnd);