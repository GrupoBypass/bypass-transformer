USE bypass_registry;

INSERT INTO LINHA (NUMERO, COR_IDENTIFICACAO, ESTACAO_ORIGEM, ESTACAO_DESTINO, EXTENSAO_KM) VALUES
(7, 'Rubi', 'Luz', 'Francisco Morato', 32.5),
(8, 'Diamante', 'Júlio Prestes', 'Amador Bueno', 35.2),
(9, 'Esmeralda', 'Osasco', 'Grajaú', 37.3),
(10, 'Turquesa', 'Brás', 'Rio Grande da Serra', 38.7),
(11, 'Coral', 'Luz', 'Mogi das Cruzes', 42.3),
(12, 'Safira', 'Brás', 'Calmon Viana', 36.8);

INSERT INTO ESTACAO (NOME, STATUS_OPERACIONAL) VALUES
-- Line 7 (Rubi)
('Luz', 'ATIVA'),
('Barra Funda', 'ATIVA'),
('Água Branca', 'ATIVA'),
('Lapa', 'ATIVA'),
('Domingos de Moraes', 'ATIVA'),
('Leopoldina', 'ATIVA'),
('Palmeiras-Barra Funda', 'ATIVA'),
('Francisco Morato', 'ATIVA'),

-- Line 8 (Diamante)
('Júlio Prestes', 'ATIVA'),
('Osasco', 'ATIVA'),
('Comandante Sampaio', 'ATIVA'),
('Presidente Altino', 'ATIVA'),
('Carapicuíba', 'ATIVA'),
('Santa Terezinha', 'ATIVA'),
('Antônio João', 'ATIVA'),
('Barueri', 'ATIVA'),
('Amador Bueno', 'ATIVA'),

-- Line 9 (Esmeralda)
('Osasco', 'ATIVA'),
('Autódromo', 'ATIVA'),
('Engenheiro Goulart', 'ATIVA'),
('Vila Natal', 'ATIVA'),
('Grajaú', 'ATIVA'),

-- Line 10 (Turquesa)
('Brás', 'ATIVA'),
('Tatuapé', 'ATIVA'),
('São Caetano do Sul', 'ATIVA'),
('Utinga', 'ATIVA'),
('Santo André', 'ATIVA'),
('Capuava', 'ATIVA'),
('Mauá', 'ATIVA'),
('Rio Grande da Serra', 'ATIVA'),

-- Line 11 (Coral)
('Luz', 'ATIVA'),
('Brás', 'ATIVA'),
('Guainazes', 'ATIVA'),
('Ferraz de Vasconcelos', 'ATIVA'),
('Poá', 'ATIVA'),
('Mogi das Cruzes', 'ATIVA'),

-- Line 12 (Safira)
('Brás', 'ATIVA'),
('Tatuapé', 'ATIVA'),
('Engenheiro Manoel Feio', 'ATIVA'),
('Itaim Paulista', 'ATIVA'),
('Calmon Viana', 'ATIVA');

INSERT INTO PLATAFORMA (NUMERO_PLATAFORMA, ID_ESTACAO) VALUES
-- Luz Station (multiple platforms)
(1, 1), (2, 1), (3, 1), (4, 1),
-- Other major stations
(1, 2), (2, 2),
(1, 3), (2, 3),
-- Continue for other stations...
(1, 4), (2, 4),
(1, 5), (2, 5);

INSERT INTO LINHA_ESTACAO (ID_LINHA, ID_ESTACAO) VALUES
-- Line 7 Stations
(1, 1), (1, 2), (1, 3), (1, 4), (1, 5), (1, 6), (1, 7), (1, 8),
-- Line 8 Stations
(2, 9), (2, 10), (2, 11), (2, 12), (2, 13), (2, 14), (2, 15), (2, 16), (2, 17),
-- Line 9 Stations
(3, 18), (3, 19), (3, 20), (3, 21), (3, 22),
-- Line 10 Stations
(4, 23), (4, 24), (4, 25), (4, 26), (4, 27), (4, 28), (4, 29), (4, 30),
-- Line 11 Stations
(5, 31), (5, 32), (5, 33), (5, 34), (5, 35), (5, 36),
-- Line 12 Stations
(6, 37), (6, 38), (6, 39), (6, 40), (6, 41);

INSERT INTO TREM (NUM_IDENTIFICACAO, MODELO, DATA_FABRICACAO, CAPACIDADE_PASSAGEIROS, STATUS_OPERACIONAL, ID_LINHA) VALUES
-- Line 7 Trains
('T701', 'Siemens CBTC', '2015-03-15', 1500, 'ATIVO', 1),
('T702', 'Siemens CBTC', '2015-06-22', 1500, 'ATIVO', 1),
('T703', 'Siemens CBTC', '2015-09-10', 1500, 'MANUTENCAO', 1),

-- Line 8 Trains
('T801', 'Bombardier MOVIA', '2014-05-18', 1800, 'ATIVO', 2),
('T802', 'Bombardier MOVIA', '2014-08-30', 1800, 'ATIVO', 2),
('T803', 'Bombardier MOVIA', '2014-11-12', 1800, 'ATIVO', 2),

-- Line 9 Trains
('T901', 'Alstom Metropolis', '2018-02-05', 2000, 'ATIVO', 3),
('T902', 'Alstom Metropolis', '2018-04-20', 2000, 'ATIVO', 3),

-- Line 10 Trains
('T1001', 'CAF CIVIA', '2016-07-15', 1600, 'ATIVO', 4),
('T1002', 'CAF CIVIA', '2016-09-28', 1600, 'ATIVO', 4),

-- Line 11 Trains
('T1101', 'Mitsui/Siemens', '2012-01-10', 1400, 'ATIVO', 5),
('T1102', 'Mitsui/Siemens', '2012-03-25', 1400, 'ATIVO', 5),

-- Line 12 Trains
('T1201', 'CAF CIVIA', '2017-05-12', 1700, 'ATIVO', 6),
('T1202', 'CAF CIVIA', '2017-08-08', 1700, 'DESATIVADO', 6);

INSERT INTO CARRO (NUM_IDENTIFICACAO, MODELO, CAPACIDADE, STATUS_OPERACIONAL, DATA_FABRICACAO) VALUES
-- Cars for Line 7 Trains
('C70101', 'Siemens CBTC Car', 250, 'ATIVO', '2015-02-10'),
('C70102', 'Siemens CBTC Car', 250, 'ATIVO', '2015-02-10'),
('C70103', 'Siemens CBTC Car', 250, 'ATIVO', '2015-02-10'),
('C70104', 'Siemens CBTC Car', 250, 'ATIVO', '2015-02-10'),
('C70105', 'Siemens CBTC Car', 250, 'ATIVO', '2015-02-10'),
('C70106', 'Siemens CBTC Car', 250, 'MANUTENCAO', '2015-02-10'),
-- Cars for Line 7 Trains
('C70107', 'Siemens CBTC Car', 250, 'ATIVO', '2015-02-10'),
('C70108', 'Siemens CBTC Car', 250, 'ATIVO', '2015-02-10'),
('C70109', 'Siemens CBTC Car', 250, 'ATIVO', '2015-02-10'),
('C70110', 'Siemens CBTC Car', 250, 'ATIVO', '2015-02-10'),
('C70111', 'Siemens CBTC Car', 250, 'ATIVO', '2015-02-10'),
('C70112', 'Siemens CBTC Car', 250, 'ATIVO', '2015-02-10'),

-- Cars for Line 8 Trains
('C80101', 'Bombardier MOVIA Car', 300, 'ATIVO', '2014-04-15'),
('C80102', 'Bombardier MOVIA Car', 300, 'ATIVO', '2014-04-15'),
('C80103', 'Bombardier MOVIA Car', 300, 'ATIVO', '2014-04-15'),
('C80104', 'Bombardier MOVIA Car', 300, 'ATIVO', '2014-04-15'),
('C80105', 'Bombardier MOVIA Car', 300, 'ATIVO', '2014-04-15'),
('C80106', 'Bombardier MOVIA Car', 300, 'ATIVO', '2014-04-15');

-- INSERT INTO COMPOSICAO_TRENS (ID_TREM, ID_CARRO, DATA_HORA_INICIO, DATA_HORA_FIM) VALUES
-- Composition for Line 8 Train 1 (T801)
-- (4, 7, '2023-03-20 09:15:00', NULL),
-- (4, 8, '2023-03-20 09:15:00', NULL),
-- (4, 9, '2023-03-20 09:15:00', NULL),
-- (4, 10, '2023-03-20 09:15:00', NULL),
-- (4, 11, '2023-03-20 09:15:00', NULL),
-- (4, 12, '2023-03-20 09:15:00', NULL);

INSERT INTO COMPOSICAO_TRENS (ID_TREM, ID_CARRO, DATA_HORA_INICIO, DATA_HORA_FIM) VALUES
-- Composition for Line 8 Train 1 (T801)
(4, 7, NOW(), NULL),
(4, 8, NOW(), NULL),
(4, 9, NOW(), NULL),
(4, 10, NOW(), NULL),
(4, 11, NOW(), NULL),
(4, 12, NOW(), NULL),
(1, 1, NOW(), NULL),
(1, 2, NOW(), NULL),
(1, 3, NOW(), NULL),
(1, 4, NOW(), NULL),
(1, 5, NOW(), NULL),
(1, 6, NOW(), NULL);
-- (2, 19, NOW(), NULL),
-- (2, 20, NOW(), NULL),
-- (2, 21, NOW(), NULL),
-- (2, 22, NOW(), NULL),
-- (2, 23, NOW(), NULL),
-- (2, 24, NOW(), NULL);

INSERT INTO CIRCUITO (MODELO, CATEGORIA, PRIORIDADE, ID_CARRO) VALUES
('Siemens S7-1500', 'Controle Principal', 1, 7),
('Siemens ET200SP', 'IO Distribuído', 2, 7),
('Siemens S7-1200', 'Sistema Auxiliar', 3, 7),
('Siemens S7-1500', 'Controle Principal', 1, 8),
('Siemens ET200SP', 'IO Distribuído', 2, 8),
('Siemens S7-1200', 'Sistema Auxiliar', 3, 8);

INSERT INTO SENSOR (ID_TREM, ID_CARRO, ID_CIRCUITO, ID_PLATAFORMA, TIPO_SENSOR, FABRICANTE, MODELO_SENSOR) VALUES
-- Sensors for Line 7 Train 1
(NULL, 1, NULL, NULL, 'TOF', 'Texas Instruments', 'OPT3101'),
(NULL, 1, NULL, NULL, 'TEMPERATURA/UMIDADE', 'Honeywell', 'HIH-4000'),
(NULL, NULL, NULL, NULL, 'INFRAVERMELHO', 'Omron', 'E3Z'),
(NULL, NULL, NULL, NULL, 'OPTICO', 'Banner', 'QS18'),

-- Sensors for Line 8 Train 1
(NULL, NULL, 1, NULL, 'DPS', 'Siemens', 'SITRANS P'),
(NULL, NULL, NULL, NULL, 'PIEZO', 'PCB Piezotronics', '102B'),

-- Sensors for Line 9 Train 1
(NULL, 1, NULL, NULL, 'TEMPERATURA/UMIDADE', 'Sensirion', 'SHT31'),
(NULL, 2, NULL, NULL, 'TOF', 'STMicroelectronics', 'VL53L0X');
