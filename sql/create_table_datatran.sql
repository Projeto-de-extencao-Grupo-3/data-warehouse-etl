-- Criar tabela para suportar dados de acidentes de trânsito
CREATE TABLE IF NOT EXISTS refined_datatran (
    ID INT PRIMARY KEY NOT NULL,
    DATA DATE NOT NULL,
    DIA_SEMANA VARCHAR(20) NOT NULL,
    HORARIO TIME NOT NULL,
    ESTADO VARCHAR(2) NOT NULL,
    MUNICIPIO VARCHAR(100) NOT NULL,
    CAUSA_ACIDENTE VARCHAR(255) NOT NULL,
    TIPO_ACIDENTE VARCHAR(100) NOT NULL,
    CLASSIFICACAO_ACIDENTE VARCHAR(100) NOT NULL,
    FASE_DIA VARCHAR(50) NOT NULL,
    SENTIDO_VIA VARCHAR(50) NOT NULL,
    CONDICAO_METEREOLOGICA VARCHAR(100) NOT NULL,
    TIPO_PISTA VARCHAR(50) NOT NULL,
    TRACADO_VIA VARCHAR(50) NOT NULL,
    PESSOAS INT NOT NULL,
    MORTOS INT NOT NULL,
    ILESOS INT NOT NULL,
    FERIDOS INT NOT NULL,
    VEICULOS INT NOT NULL,
    REGIONAL VARCHAR(10) NOT NULL,
    MES INT NOT NULL,
    INDEX idx_data (DATA),
    INDEX idx_estado (ESTADO),
    INDEX idx_municipio (MUNICIPIO)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Inserir dados de exemplo
INSERT INTO refined_datatran VALUES
(331693, '2021-01-01', 'sexta-feira', '00:02:00', 'SC', 'LAGUNA', 'Ingestão de álcool pelo condutor', 'Colisão traseira', 'Com Vítimas Feridas', 'Plena Noite', 'Crescente', 'Nublado', 'Dupla', 'Reta', 3, 0, 2, 1, 2, 'SR-SC', 1),
(331694, '2021-01-01', 'sexta-feira', '00:15:00', 'PR', 'MARIALVA', 'Velocidade Incompatível', 'Colisão com objeto', 'Com Vítimas Feridas', 'Plena Noite', 'Crescente', 'Céu Claro', 'Dupla', 'Reta', 2, 0, 0, 2, 1, 'SR-PR', 1),
(331696, '2021-01-01', 'sexta-feira', '00:00:00', 'SP', 'TAUBATE', 'Demais falhas mecânicas ou elétricas', 'Incêndio', 'Sem Vítimas', 'Plena Noite', 'Crescente', 'Nublado', 'Dupla', 'Reta', 1, 0, 1, 0, 1, 'SR-SP', 1),
(331699, '2021-01-01', 'sexta-feira', '01:20:00', 'SC', 'NAVEGANTES', 'Manobra de mudança de faixa', 'Colisão lateral', 'Com Vítimas Feridas', 'Plena Noite', 'Crescente', 'Nublado', 'Múltipla', 'Curva', 3, 0, 1, 2, 2, 'SR-SC', 1);
