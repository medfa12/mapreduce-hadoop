sequenceDiagram
    title Phase d'authentification et échange des données

    participant V as Véhicule Vᵢ
    participant R as RSU
    
    Note over V,R: <b>Phase 1 : Authentification</b>
    
    V->>R: (1) Hello, je suis Vᵢ (envoi ID)
    R->>R: Génère un nonce (N)
    R-->>V: (2) Envoi de N
    V->>V: Calculer Réponse = MAC<sub>Kᵥᵢ</sub>(N)
    V-->>R: (3) Envoi de la Réponse
    R->>R: Vérifie Réponse avec Kᵥᵢ<br/>Si OK → Authentification réussie
    R-->>V: Accusé de réception ou<br/>négociation K<sub>session</sub>

    Note over V,R: <b>Phase 2 : Échange de données (D) sécurisé</b>

    V->>V: Chiffrer D avec K<sub>session</sub><br/>Calculer MAC<sub>K<sub>session</sub></sub>(D)
    V-->>R: (4) E<sub>K<sub>session</sub></sub>(D) + MAC
    R->>R: Déchiffrer avec K<sub>session</sub><br/>Vérifier MAC
    R->>R: Préparer réponse<br/>(Ack ou autre)
    R-->>V: (5) E<sub>K<sub>session</sub></sub>(Ack) + MAC
    
    Note over V,R: Les échanges ultérieurs sont chiffrés et vérifiés (MAC) avec K<sub>session</sub>.
