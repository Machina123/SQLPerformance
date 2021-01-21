## Machina123/SQLPerformance
Skrypt testujący wydajność baz danych MySQL i MariaDB - część pracy inżynierskiej pt. "Porównanie baz danych MySQL i MariaDB w testach obciążeniowych."

### Uruchomienie
1. Wymagana jest [instalacja środowiska Docker](https://www.docker.com/get-started) dla wybranego systemu operacyjnego
2. Naley pobrać całą zawartość niniejszego repozytorium komendą
    ```
   git clone https://github.com/Machina123/SQLPerformance.git --recursive
    ```
3. Uruchomienie skryptu następuje poprzez wykonanie w linii komend polecenia
   ```
   docker-compose up
   ```
   W przypadku, gdyby kontener ze skryptem wyłączył się z powodu błędu połączenia z bazą danych, można go uruchomić poleceniem
    ```
    docker-compose start test-script
    ```
4. Wyłączenie aplikacji następuje po wykonaniu komendy
    ```
   docker-compose down
   ```

### Licencja
Projekt udostępniany jest na licencji MIT, szczegóły [zawarte są tutaj](./LICENSE).