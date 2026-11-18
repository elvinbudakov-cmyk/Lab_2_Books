
const API_URL = "https://openlibrary.org/search.json";

const searchInput = document.getElementById("search");
const searchButton = document.getElementById("search-btn");
const statusText = document.getElementById("status");

const titleResults = document.getElementById("title-results");
const authorResults = document.getElementById("author-results");
const genreResults = document.getElementById("genre-results");

const { fromEvent, merge, from, of } = rxjs;

const {
    map,
    filter,
    debounceTime,
    distinctUntilChanged,
    switchMap,
    finalize
} = rxjs.operators;

function createUrl(type, query) {
    const params = new URLSearchParams();

    params.set(type, query);
    params.set("limit", "10");
    params.set(
        "fields",
        "key,title,author_name,cover_i,first_publish_year,subject"
    );

    return API_URL + "?" + params.toString();
}

async function fetchBooks(type, query, signal) {
    const response = await fetch(createUrl(type, query), {
        signal: signal
    });

    if (!response.ok) {
        throw new Error("HTTP " + response.status);
    }

    const data = await response.json();

    return data.docs || [];
}

function showMessage(container, message) {
    container.replaceChildren();

    const p = document.createElement("p");
    p.className = "empty-message";
    p.textContent = message;

    container.appendChild(p);
}

function createBook(book) {
    const item = document.createElement("div");
    item.className = "book-item";

    if (book.cover_i) {
        const image = document.createElement("img");

        image.src =
            "https://covers.openlibrary.org/b/id/" +
            book.cover_i + "-M.jpg";

        image.alt = "Обкладинка книги";
        image.loading = "lazy";

        item.appendChild(image);
    }

    const info = document.createElement("div");
    info.className = "book-info";

    const title = document.createElement("h3");
    title.textContent = book.title || "Без назви";

    const author = document.createElement("p");
    author.textContent = "Автор: " +
        (book.author_name?.join(", ") || "Невідомий");

    const year = document.createElement("p");
    year.textContent = "Рік першого видання: " +
        (book.first_publish_year || "Невідомий");

    const subject = document.createElement("p");

    subject.textContent = "Тематика: " +
        (book.subject?.slice(0, 3).join(", ") ||
        "Не вказано");

    info.append(title, author, year, subject);
    
if (/^\/works\/OL\d+W$/.test(book.key || "")) {
    const descriptionButton = document.createElement("button");
    descriptionButton.textContent = "Показати опис";

    const description = document.createElement("p");

    descriptionButton.addEventListener("click", async () => {
        descriptionButton.disabled = true;
        description.textContent = "Завантаження опису...";

        try {
            const response = await fetch(
                "https://openlibrary.org" + book.key + ".json"
            );

            if (!response.ok) {
                throw new Error("HTTP " + response.status);
            }

            const data = await response.json();

            const text = typeof data.description === "string"
                ? data.description
                : data.description?.value;

            description.textContent =
                text || "Опис цієї книги відсутній.";
        } catch (error) {
            description.textContent =
                "Не вдалося завантажити опис: " + error.message;
        }

        descriptionButton.remove();
    });

    info.append(descriptionButton, description);
}


    if (/^\/works\/OL\d+W$/.test(book.key || "")) {
        const link = document.createElement("a");

        link.href = "https://openlibrary.org" + book.key;
        link.target = "_blank";
        link.rel = "noopener noreferrer";
        link.textContent = "Детальніше про книгу";

        info.appendChild(link);
    }

    item.appendChild(info);

    return item;
}

function renderBooks(container, books) {
    container.replaceChildren();

    if (books.length === 0) {
        showMessage(container, "Книг не знайдено.");
        return;
    }

    books.forEach(book => {
        container.appendChild(createBook(book));
    });
}

async function delayedSearch(type, query, delay, signal) {
    await new Promise(resolve => setTimeout(resolve, delay));

    if (signal.aborted) {
        return { books: [], cancelled: true };
    }

    try {
        const books = await fetchBooks(type, query, signal);

        return { books: books, error: null };
    } catch (error) {
        return { books: [], error: error };
    }
}

async function searchBooks(query, signal) {
    const requests = [
        delayedSearch("title", query, 0, signal),
        delayedSearch("author", query, 1200, signal),
        delayedSearch("subject", query, 2400, signal)
    ];

    return Promise.all(requests);
}

function displayResults(results) {
    const containers = [
        titleResults,
        authorResults,
        genreResults
    ];

    let total = 0;
    let errors = 0;

    results.forEach((result, index) => {
        if (result.error) {
            errors++;

            showMessage(
                containers[index],
                "Помилка завантаження: " +
                result.error.message
            );
        } else {
            renderBooks(containers[index], result.books);
            total += result.books.length;
        }
    });

    if (errors === 3) {
        statusText.textContent =
            "Не вдалося завантажити книги. Перевірте з'єднання.";
    } else {
        statusText.textContent =
            "Показано книг: " + total +
            (errors ? ". Частина запитів не виконана." : "");
    }
}

function clearResults(message) {
    statusText.textContent = message;

    titleResults.replaceChildren();
    authorResults.replaceChildren();
    genreResults.replaceChildren();
}

const input$ = fromEvent(searchInput, "input").pipe(
    map(() => searchInput.value.trim()),
    debounceTime(300),
    distinctUntilChanged(),
    filter(value => value.length === 0 || value.length >= 3)
);

const button$ = fromEvent(searchButton, "click").pipe(
    map(() => searchInput.value.trim())
);

const enter$ = fromEvent(searchInput, "keydown").pipe(
    filter(event => event.key === "Enter"),
    map(event => {
        event.preventDefault();
        return searchInput.value.trim();
    })
);

merge(input$, button$, enter$).pipe(
    switchMap(query => {
        if (!query) {
            clearResults("Введіть запит для пошуку книг.");
            return of(null);
        }

        clearResults("Завантаження книг...");

        const controller = new AbortController();

        return from(
            searchBooks(query, controller.signal)
        ).pipe(
            finalize(() => controller.abort())
        );
    })
).subscribe(results => {
    if (results) {
        displayResults(results);
    }
});
