
#define NOMINMAX
#define WIN32_LEAN_AND_MEAN
#define PAHO_MQTTPP_IMPORTS

#include <iostream>
#include <iomanip>
#include <fstream>
#include <string>
#include <set>
#include <vector>
#include <chrono>
#include <thread>

#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
#pragma comment(lib, "Ws2_32.lib")
#endif

#ifdef max
#undef max
#endif

#ifdef min
#undef min
#endif

#include <mqtt/async_client.h>

using namespace std;

// Algemene instellingen
const string BROKER = "tcp://localhost:1883";
const string REQUEST_TOPIC = "robotfleet/overdracht";
const string ACK_TOPIC = "robotfleet/bevestiging";

const int PORT = 5050;
const int AANTAL_METINGEN = 30;

set<string> verwerkteBerichten;

// berekenen van resultaten

void toonSamenvatting(const vector<double>& tijden, const string& methode)
{
    if (tijden.empty())
    {
        cout << "Geen geslaagde metingen voor " << methode << endl;
        return;
    }

    double totaal = 0;
    double laagste = tijden[0];
    double hoogste = tijden[0];

    for (double tijd : tijden)
    {
        totaal += tijd;

        if (tijd < laagste)
            laagste = tijd;

        if (tijd > hoogste)
            hoogste = tijd;
    }

    cout << fixed << setprecision(3);

    cout << "\n--- Resultaten " << methode << " ---" << endl;
    cout << "Geslaagde metingen: "
        << tijden.size() << "/" << AANTAL_METINGEN << endl;

    cout << "Gemiddelde reactietijd: "
        << totaal / tijden.size() << " ms" << endl;

    cout << "Laagste reactietijd: "
        << laagste << " ms" << endl;

    cout << "Hoogste reactietijd: "
        << hoogste << " ms" << endl;
}



bool verstuurTcp(SOCKET verbinding, const string& tekst)
{
    string data = tekst + "\n";
    size_t verzonden = 0;

    while (verzonden < data.size())
    {
        int aantal = send(
            verbinding,
            data.data() + verzonden,
            static_cast<int>(data.size() - verzonden),
            0
        );

        if (aantal <= 0)
            return false;

        verzonden += aantal;
    }

    return true;
}

bool ontvangTcp(SOCKET verbinding, string& buffer, string& bericht)
{
    size_t einde;

    while ((einde = buffer.find('\n')) == string::npos)
    {
        char tijdelijk[512];

        int aantal = recv(
            verbinding,
            tijdelijk,
            sizeof(tijdelijk),
            0
        );

        if (aantal <= 0)
            return false;

        buffer.append(tijdelijk, aantal);
    }

    bericht = buffer.substr(0, einde);
    buffer.erase(0, einde + 1);

    return true;
}

// tcp zone b

void tcpServer()
{
    WSADATA wsaData;

    if (WSAStartup(MAKEWORD(2, 2), &wsaData) != 0)
    {
        cout << "Winsock kon niet starten." << endl;
        return;
    }

    SOCKET serverSocket = socket(
        AF_INET,
        SOCK_STREAM,
        IPPROTO_TCP
    );

    if (serverSocket == INVALID_SOCKET)
    {
        cout << "Socket fout: " << WSAGetLastError() << endl;
        WSACleanup();
        return;
    }

    sockaddr_in adres{};
    adres.sin_family = AF_INET;
    adres.sin_addr.s_addr = INADDR_ANY;
    adres.sin_port = htons(PORT);

    if (::bind(serverSocket, (sockaddr*)&adres, sizeof(adres)) == SOCKET_ERROR)
    {
        cout << "Bind fout: " << WSAGetLastError() << endl;

        closesocket(serverSocket);
        WSACleanup();
        return;
    }

    if (listen(serverSocket, 1) == SOCKET_ERROR)
    {
        cout << "Listen fout: " << WSAGetLastError() << endl;

        closesocket(serverSocket);
        WSACleanup();
        return;
    }

    cout << "TCP Zone B wacht op Zone A..." << endl;

    SOCKET clientSocket = accept(
        serverSocket,
        nullptr,
        nullptr
    );

    if (clientSocket == INVALID_SOCKET)
    {
        cout << "Accept fout: " << WSAGetLastError() << endl;

        closesocket(serverSocket);
        WSACleanup();
        return;
    }

    string buffer;
    string bericht;

    // Ontvang meerdere berichten
    while (ontvangTcp(clientSocket, buffer, bericht))
    {
        string id = bericht.substr(0, bericht.find(';'));

        bool nieuw = verwerkteBerichten.insert(id).second;

        // Bevestiging terugsturen
        if (!verstuurTcp(clientSocket, id + ";ONTVANGEN"))
            break;

        cout << "TCP ontvangen: " << bericht;

        if (nieuw)
            cout << " (verwerkt)" << endl;
        else
            cout << " (dubbel, overgeslagen)" << endl;
    }

    cout << "TCP-verbinding afgesloten." << endl;

    closesocket(clientSocket);
    closesocket(serverSocket);
    WSACleanup();
}

// tcp zone a

void tcpClient()
{
    WSADATA wsaData;

    if (WSAStartup(MAKEWORD(2, 2), &wsaData) != 0)
        return;

    SOCKET verbinding = socket(
        AF_INET,
        SOCK_STREAM,
        IPPROTO_TCP
    );

    if (verbinding == INVALID_SOCKET)
    {
        cout << "Socket maken mislukt." << endl;
        WSACleanup();
        return;
    }

    sockaddr_in adres{};
    adres.sin_family = AF_INET;
    adres.sin_port = htons(PORT);

    inet_pton(
        AF_INET,
        "127.0.0.1",
        &adres.sin_addr
    );

    if (connect(verbinding, (sockaddr*)&adres, sizeof(adres)) == SOCKET_ERROR)
    {
        cout << "TCP-verbinding met Zone B mislukt." << endl;

        closesocket(verbinding);
        WSACleanup();
        return;
    }

    // Resultaten opslaan in CSV
    ofstream csv("tcp_metingen.csv");
    csv << "Test;Reactietijd_ms;Status\n";

    vector<double> tijden;
    string buffer;

    // Uniek ID voor iedere test
    string reeks = to_string(
        chrono::steady_clock::now().time_since_epoch().count()
    );

    cout << "TCP: " << AANTAL_METINGEN
        << " overdrachten testen..." << endl;

    for (int i = 1; i <= AANTAL_METINGEN; i++)
    {
        string id = reeks + "-" + to_string(i);
        string verzoek = id + ";PALLET_OVERDRACHT";
        string antwoord;

        // Tijdmeting starten
        auto start = chrono::steady_clock::now();

        bool gelukt =
            verstuurTcp(verbinding, verzoek) &&
            ontvangTcp(verbinding, buffer, antwoord);

        // Tijdmeting stoppen
        auto einde = chrono::steady_clock::now();

        if (!gelukt || antwoord != id + ";ONTVANGEN")
        {
            cout << "Test " << i
                << ": geen geldige bevestiging." << endl;

            csv << i << ";;MISLUKT\n";
            break;
        }

        // Reactietijd in milliseconden
        double ms = chrono::duration<double, milli>(
            einde - start
        ).count();

        tijden.push_back(ms);

        csv << i << ";"
            << fixed << setprecision(3)
            << ms << ";OK\n";

        cout << fixed << setprecision(3);
        cout << "Test " << i << ": " << ms << " ms" << endl;

        this_thread::sleep_for(chrono::milliseconds(20));
    }

    toonSamenvatting(tijden, "TCP");

    cout << "Opgeslagen in tcp_metingen.csv" << endl;

    closesocket(verbinding);
    WSACleanup();
}

// mqtt zone b

void mqttServer()
{
    try
    {
        mqtt::async_client client(BROKER, "ZoneB");

        mqtt::connect_options opties;
        client.connect(opties)->wait();

        client.start_consuming();
        client.subscribe(REQUEST_TOPIC, 1)->wait();

        cout << "MQTT Zone B wacht op berichten..." << endl;

        // Server blijft berichten ontvangen
        while (true)
        {
            auto message = client.try_consume_message_for(
                chrono::seconds(1)
            );

            if (!message)
                continue;

            string bericht = message->to_string();
            string id = bericht.substr(0, bericht.find(';'));

            // Controle op dubbele opdrachten
            bool nieuw = verwerkteBerichten.insert(id).second;

            // Bevestiging terugsturen
            auto bevestiging = mqtt::make_message(
                ACK_TOPIC,
                id + ";ONTVANGEN"
            );

            bevestiging->set_qos(1);
            client.publish(bevestiging)->wait();

            cout << "MQTT ontvangen: " << bericht;

            if (nieuw)
                cout << " (verwerkt)" << endl;
            else
                cout << " (dubbel, overgeslagen)" << endl;
        }
    }
    catch (const mqtt::exception& e)
    {
        cout << "MQTT-fout: " << e.what() << endl;
    }
}

// mqtt zone a

void mqttClient()
{
    try
    {
        mqtt::async_client client(BROKER, "ZoneA");

        mqtt::connect_options opties;
        client.connect(opties)->wait();

        client.start_consuming();
        client.subscribe(ACK_TOPIC, 1)->wait();

        // Resultaten opslaan in CSV
        ofstream csv("mqtt_metingen.csv");
        csv << "Test;Reactietijd_ms;Status\n";

        vector<double> tijden;

        string reeks = to_string(
            chrono::steady_clock::now().time_since_epoch().count()
        );

        cout << "MQTT: " << AANTAL_METINGEN
            << " overdrachten testen..." << endl;

        for (int i = 1; i <= AANTAL_METINGEN; i++)
        {
            string id = reeks + "-" + to_string(i);

            auto verzoek = mqtt::make_message(
                REQUEST_TOPIC,
                id + ";PALLET_OVERDRACHT"
            );

            verzoek->set_qos(1);

            // Tijdmeting starten
            auto start = chrono::steady_clock::now();

            client.publish(verzoek)->wait();

            bool gelukt = false;

            auto deadline = start + chrono::seconds(10);

            while (chrono::steady_clock::now() < deadline)
            {
                auto antwoord = client.try_consume_message_for(
                    chrono::seconds(1)
                );

                if (antwoord &&
                    antwoord->to_string() == id + ";ONTVANGEN")
                {
                    // Tijdmeting stoppen
                    auto einde = chrono::steady_clock::now();

                    double ms = chrono::duration<double, milli>(
                        einde - start
                    ).count();

                    tijden.push_back(ms);

                    csv << i << ";"
                        << fixed << setprecision(3)
                        << ms << ";OK\n";

                    cout << fixed << setprecision(3);
                    cout << "Test " << i << ": "
                        << ms << " ms" << endl;

                    gelukt = true;
                    break;
                }
            }

            if (!gelukt)
            {
                cout << "Test " << i
                    << ": geen geldige bevestiging." << endl;

                csv << i << ";;MISLUKT\n";
                break;
            }

            this_thread::sleep_for(chrono::milliseconds(20));
        }

        toonSamenvatting(tijden, "MQTT QoS 1");

        cout << "Opgeslagen in mqtt_metingen.csv" << endl;

        client.stop_consuming();
        client.disconnect()->wait();
    }
    catch (const mqtt::exception& e)
    {
        cout << "MQTT-fout: " << e.what() << endl;
    }
}

// main code

int main(int argc, char* argv[])
{
    if (argc < 2)
    {
        cout << "Gebruik een van deze commando's:" << endl;
        cout << "tcp-server" << endl;
        cout << "tcp-client" << endl;
        cout << "mqtt-server" << endl;
        cout << "mqtt-client" << endl;

        return 1;
    }

    string keuze = argv[1];

    if (keuze == "tcp-server")
    {
        tcpServer();
    }
    else if (keuze == "tcp-client")
    {
        tcpClient();
    }
    else if (keuze == "mqtt-server")
    {
        mqttServer();
    }
    else if (keuze == "mqtt-client")
    {
        mqttClient();
    }
    else
    {
        cout << "Ongeldige keuze." << endl;
        return 1;
    }

    return 0;
}
