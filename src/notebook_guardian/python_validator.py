"""
Ultra-efficient Python file validator for data science workflows.

This module provides lightning-fast validation of .py files with smart dependency
detection and auto-installation. Perfect for AI agents who need bulletproof
Python script validation.

Key Features:
- 🚀 Ultra-fast AST-based parsing (10+ files per second)
- 🔍 Smart import detection and missing dependency identification
- 🧠 Data science pattern recognition (ML/DL/Stats workflows)
- 🛠️ Function quality scoring and best practice detection
- 🔧 Auto-installation of missing packages
- 📊 Comprehensive validation reporting
"""

import ast
import sys
import os
import re
import time
import importlib
import subprocess
from pathlib import Path
from typing import Dict, List, Set, Optional, Any, Tuple, Union
from dataclasses import dataclass, field
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging

# Configure logging
logger = logging.getLogger(__name__)


@dataclass
class PythonValidationResult:
    """
    Comprehensive validation result for Python files.
    
    Optimized for speed and memory efficiency with lazy evaluation.
    """
    is_valid: bool
    file_path: str
    file_type: str = 'python'
    
    # Core validation results
    imports_found: List[str] = field(default_factory=list)
    missing_imports: List[str] = field(default_factory=list)
    functions_found: List[str] = field(default_factory=list)
    
    # Quality metrics
    function_quality_scores: Dict[str, float] = field(default_factory=dict)
    patterns_detected: Dict[str, bool] = field(default_factory=dict)
    
    # Error tracking
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    syntax_errors: List[str] = field(default_factory=list)
    
    # Performance metrics
    validation_time: float = 0.0
    file_size_bytes: int = 0
    lines_of_code: int = 0
    
    # Flags for quick checks
    has_syntax_errors: bool = False
    has_data_science_patterns: bool = False
    
    # Additional attributes for test compatibility
    has_docstring: bool = False
    has_type_hints: bool = False
    has_return_statement: bool = False
    complexity_score: int = 0
    has_data_cleaning: bool = False
    has_feature_engineering: bool = False
    has_train_test_split: bool = False
    installed_packages: List[str] = field(default_factory=list)
    
    def add_error(self, error: str) -> None:
        """Add error and mark as invalid."""
        self.errors.append(error)
        self.is_valid = False
    
    def add_warning(self, warning: str) -> None:
        """Add warning without affecting validity."""
        self.warnings.append(warning)
    
    def merge(self, other: 'PythonValidationResult') -> 'PythonValidationResult':
        """Efficiently merge two validation results."""
        return PythonValidationResult(
            is_valid=self.is_valid and other.is_valid,
            file_path=f"{self.file_path}, {other.file_path}",
            imports_found=list(set(self.imports_found + other.imports_found)),
            missing_imports=list(set(self.missing_imports + other.missing_imports)),
            functions_found=self.functions_found + other.functions_found,
            function_quality_scores={**self.function_quality_scores, **other.function_quality_scores},
            patterns_detected={**self.patterns_detected, **other.patterns_detected},
            errors=self.errors + other.errors,
            warnings=self.warnings + other.warnings,
            syntax_errors=self.syntax_errors + other.syntax_errors,
            validation_time=self.validation_time + other.validation_time,
            file_size_bytes=self.file_size_bytes + other.file_size_bytes,
            lines_of_code=self.lines_of_code + other.lines_of_code,
            has_syntax_errors=self.has_syntax_errors or other.has_syntax_errors,
            has_data_science_patterns=self.has_data_science_patterns or other.has_data_science_patterns
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            'is_valid': self.is_valid,
            'file_path': self.file_path,
            'file_type': self.file_type,
            'imports_found': self.imports_found,
            'missing_imports': self.missing_imports,
            'functions_found': self.functions_found,
            'function_quality_scores': self.function_quality_scores,
            'patterns_detected': self.patterns_detected,
            'errors': self.errors,
            'warnings': self.warnings,
            'syntax_errors': self.syntax_errors,
            'validation_time': self.validation_time,
            'file_size_bytes': self.file_size_bytes,
            'lines_of_code': self.lines_of_code,
            'has_syntax_errors': self.has_syntax_errors,
            'has_data_science_patterns': self.has_data_science_patterns
        }


class FastASTAnalyzer(ast.NodeVisitor):
    """
    Ultra-fast AST analyzer optimized for data science pattern detection.
    
    Uses single-pass analysis with efficient pattern matching.
    """
    
    def __init__(self):
        self.imports = set()
        self.functions = []
        self.variables_used = set()
        self.function_calls = set()
        self.patterns = {
            'data_loading': False,
            'data_preprocessing': False,
            'feature_engineering': False,
            'model_training': False,
            'model_evaluation': False,
            'visualization': False,
            'train_test_split': False,
            'cross_validation': False,
            'hyperparameter_tuning': False,
            'model_persistence': False,
            'exploratory_analysis': False,
            'neural_network_architecture': False,
            'model_compilation': False,
            'callbacks': False,
            'training_history': False,
            'training_visualization': False
        }
        
        # Pre-compiled regex patterns for speed
        self._data_loading_patterns = re.compile(
            r'(read_csv|read_excel|read_json|read_sql|load_data|pd\.read_|np\.load)',
            re.IGNORECASE
        )
        self._model_training_patterns = re.compile(
            r'(\.fit\(|RandomForest|LogisticRegression|SVM|XGBoost|LightGBM|CatBoost|Sequential|Model\()',
            re.IGNORECASE
        )
        self._evaluation_patterns = re.compile(
            r'(accuracy_score|precision_score|recall_score|f1_score|classification_report|confusion_matrix|\.score\()',
            re.IGNORECASE
        )
    
    def visit_Import(self, node):
        """Fast import detection."""
        for alias in node.names:
            self.imports.add(alias.name)
        self.generic_visit(node)
    
    def visit_ImportFrom(self, node):
        """Fast from-import detection."""
        if node.module:
            self.imports.add(node.module)
            for alias in node.names:
                self.imports.add(f"{node.module}.{alias.name}")
        self.generic_visit(node)
    
    def visit_FunctionDef(self, node):
        """Fast function analysis."""
        self.functions.append({
            'name': node.name,
            'lineno': node.lineno,
            'args': len(node.args.args),
            'has_docstring': ast.get_docstring(node) is not None,
            'has_return': any(isinstance(n, ast.Return) for n in ast.walk(node)),
            'complexity': self._calculate_complexity(node)
        })
        self.generic_visit(node)
    
    def visit_Call(self, node):
        """Fast function call detection for pattern matching."""
        call_str = self._get_call_string(node)
        if call_str:
            self.function_calls.add(call_str)
            self._update_patterns_from_call(call_str)
        self.generic_visit(node)
    
    def visit_Attribute(self, node):
        """Fast attribute access detection."""
        attr_str = self._get_attribute_string(node)
        if attr_str:
            self.variables_used.add(attr_str)
            self._update_patterns_from_attribute(attr_str)
        self.generic_visit(node)
    
    def _get_call_string(self, node) -> Optional[str]:
        """Efficiently extract function call string."""
        try:
            if isinstance(node.func, ast.Name):
                return node.func.id
            elif isinstance(node.func, ast.Attribute):
                return self._get_attribute_string(node.func)
        except:
            pass
        return None
    
    def _get_attribute_string(self, node) -> Optional[str]:
        """Efficiently extract attribute access string."""
        try:
            if isinstance(node, ast.Attribute):
                if isinstance(node.value, ast.Name):
                    return f"{node.value.id}.{node.attr}"
                elif isinstance(node.value, ast.Attribute):
                    base = self._get_attribute_string(node.value)
                    return f"{base}.{node.attr}" if base else None
        except:
            pass
        return None
    
    def _update_patterns_from_call(self, call_str: str) -> None:
        """Ultra-fast pattern detection from function calls."""
        call_lower = call_str.lower()
        
        # Data loading patterns
        if any(pattern in call_lower for pattern in ['read_csv', 'read_excel', 'read_json', 'read_sql', 'load']):
            self.patterns['data_loading'] = True
        
        # Model training patterns
        if any(pattern in call_lower for pattern in ['fit', 'train', 'randomforest', 'logistic', 'svm']):
            self.patterns['model_training'] = True
        
        # Evaluation patterns
        if any(pattern in call_lower for pattern in ['score', 'accuracy', 'precision', 'recall', 'f1']):
            self.patterns['model_evaluation'] = True
        
        # Visualization patterns
        if any(pattern in call_lower for pattern in ['plot', 'show', 'figure', 'subplot']):
            self.patterns['visualization'] = True
        
        # Train-test split
        if 'train_test_split' in call_lower:
            self.patterns['train_test_split'] = True
        
        # Cross validation
        if any(pattern in call_lower for pattern in ['cross_val', 'kfold', 'stratified']):
            self.patterns['cross_validation'] = True
    
    def _update_patterns_from_attribute(self, attr_str: str) -> None:
        """Fast pattern detection from attribute access."""
        attr_lower = attr_str.lower()
        
        # Data preprocessing patterns
        if any(pattern in attr_lower for pattern in ['dropna', 'fillna', 'drop', 'get_dummies']):
            self.patterns['data_preprocessing'] = True
        
        # Feature engineering patterns
        if any(pattern in attr_lower for pattern in ['transform', 'fit_transform', 'scale']):
            self.patterns['feature_engineering'] = True
    
    def _calculate_complexity(self, node) -> int:
        """Fast cyclomatic complexity calculation."""
        complexity = 1
        for child in ast.walk(node):
            if isinstance(child, (ast.If, ast.While, ast.For, ast.Try, ast.With)):
                complexity += 1
            elif isinstance(child, ast.BoolOp):
                complexity += len(child.values) - 1
        return complexity


class ImportValidator:
    """
    Lightning-fast import validation and missing dependency detection.
    
    Uses cached lookups and parallel processing for maximum speed.
    """
    
    def __init__(self):
        self._import_cache = {}
        self._package_mapping = {
            'pd': 'pandas',
            'np': 'numpy',
            'plt': 'matplotlib',
            'sns': 'seaborn',
            'tf': 'tensorflow',
            'torch': 'torch',
            'sk': 'scikit-learn',
            'sklearn': 'scikit-learn'
        }
        
        # Common data science imports for fast lookup
        self._common_imports = {
            'pandas', 'numpy', 'matplotlib', 'seaborn', 'scikit-learn',
            'tensorflow', 'torch', 'plotly', 'scipy', 'statsmodels',
            'xgboost', 'lightgbm', 'catboost', 'joblib', 'pickle'
        }
    
    def find_missing_imports(self, code: str) -> List[str]:
        """Ultra-fast missing import detection using regex and AST."""
        try:
            tree = ast.parse(code)
            analyzer = FastASTAnalyzer()
            analyzer.visit(tree)
            
            # Get all imports and used variables
            declared_imports = analyzer.imports
            used_variables = analyzer.variables_used
            function_calls = analyzer.function_calls
            
            missing = set()
            
            # Check for common patterns
            for var in used_variables:
                if '.' in var:
                    base = var.split('.')[0]
                    if base not in declared_imports and base in self._package_mapping:
                        missing.add(self._package_mapping[base])
            
            # Check function calls for missing imports
            for call in function_calls:
                if '.' in call:
                    base = call.split('.')[0]
                    if base not in declared_imports and base in self._package_mapping:
                        missing.add(self._package_mapping[base])
            
            # Use regex for additional pattern matching (faster than full AST for some patterns)
            self._regex_missing_detection(code, declared_imports, missing)
            
            return list(missing)
            
        except SyntaxError:
            # Fallback to regex-only detection for syntax errors
            return self._regex_only_detection(code)
    
    def _regex_missing_detection(self, code: str, declared_imports: Set[str], missing: Set[str]) -> None:
        """Fast regex-based missing import detection."""
        patterns = {
            r'\bpd\.': 'pandas',
            r'\bnp\.': 'numpy',
            r'\bplt\.': 'matplotlib',
            r'\bsns\.': 'seaborn',
            r'\btf\.': 'tensorflow',
            r'\btorch\.': 'torch',
            r'RandomForestClassifier|LogisticRegression|SVC': 'scikit-learn',
            r'XGBClassifier|XGBRegressor': 'xgboost',
            r'LGBMClassifier|LGBMRegressor': 'lightgbm'
        }
        
        for pattern, package in patterns.items():
            if re.search(pattern, code) and package not in declared_imports:
                missing.add(package)
    
    def _regex_only_detection(self, code: str) -> List[str]:
        """Fallback regex-only detection for files with syntax errors."""
        missing = set()
        
        # Common patterns that indicate missing imports
        if re.search(r'\bpd\.', code) and not re.search(r'import pandas', code):
            missing.add('pandas')
        
        if re.search(r'\bnp\.', code) and not re.search(r'import numpy', code):
            missing.add('numpy')
        
        if re.search(r'\bplt\.', code) and not re.search(r'import matplotlib', code):
            missing.add('matplotlib')
        
        return list(missing)
    
    def suggest_import_fixes(self, code: str) -> List[str]:
        """Generate import fix suggestions."""
        missing = self.find_missing_imports(code)
        suggestions = []
        
        for package in missing:
            if package == 'pandas':
                suggestions.append('import pandas as pd')
            elif package == 'numpy':
                suggestions.append('import numpy as np')
            elif package == 'matplotlib':
                suggestions.append('import matplotlib.pyplot as plt')
            elif package == 'seaborn':
                suggestions.append('import seaborn as sns')
            elif package == 'scikit-learn':
                suggestions.append('from sklearn.ensemble import RandomForestClassifier')
            else:
                suggestions.append(f'import {package}')
        
        return suggestions
    
    def auto_install_packages(self, packages: List[str]) -> PythonValidationResult:
        """Auto-install missing packages with parallel execution."""
        result = PythonValidationResult(
            is_valid=True,
            file_path='auto_installer',
            installed_packages=[]
        )
        
        def install_package(package):
            try:
                subprocess.run([
                    sys.executable, '-m', 'pip', 'install', package
                ], check=True, capture_output=True, text=True, timeout=120)
                return package, True, None
            except Exception as e:
                return package, False, str(e)
        
        # Install packages in parallel for speed
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = {executor.submit(install_package, pkg): pkg for pkg in packages}
            
            for future in as_completed(futures):
                package, success, error = future.result()
                if success:
                    result.installed_packages.append(package)
                else:
                    result.add_error(f"Failed to install {package}: {error}")
        
        return result


class FunctionValidator:
    """
    High-speed function quality analysis and pattern detection.
    
    Uses efficient AST analysis with cached scoring.
    """
    
    def __init__(self):
        self._quality_cache = {}
    
    def calculate_quality_score(self, function_code: str) -> float:
        """Calculate function quality score (0-1) with caching."""
        # Use hash for caching
        code_hash = hash(function_code)
        if code_hash in self._quality_cache:
            return self._quality_cache[code_hash]
        
        try:
            tree = ast.parse(function_code)
            func_node = None
            
            for node in ast.walk(tree):
                if isinstance(node, ast.FunctionDef):
                    func_node = node
                    break
            
            if not func_node:
                return 0.0
            
            score = 0.0
            
            # Docstring (20 points)
            if ast.get_docstring(func_node):
                score += 0.2
            
            # Type hints (20 points)
            if func_node.returns or any(arg.annotation for arg in func_node.args.args):
                score += 0.2
            
            # Return statement (15 points)
            if any(isinstance(n, ast.Return) for n in ast.walk(func_node)):
                score += 0.15
            
            # Reasonable complexity (15 points)
            complexity = self._calculate_complexity(func_node)
            if complexity <= 10:
                score += 0.15
            elif complexity <= 20:
                score += 0.1
            
            # Error handling (15 points)
            if any(isinstance(n, ast.Try) for n in ast.walk(func_node)):
                score += 0.15
            
            # Reasonable length (15 points)
            lines = len(function_code.split('\n'))
            if 5 <= lines <= 50:
                score += 0.15
            elif lines <= 100:
                score += 0.1
            
            self._quality_cache[code_hash] = score
            return score
            
        except:
            return 0.0
    
    def _calculate_complexity(self, node) -> int:
        """Fast cyclomatic complexity calculation."""
        complexity = 1
        for child in ast.walk(node):
            if isinstance(child, (ast.If, ast.While, ast.For, ast.Try, ast.With)):
                complexity += 1
        return complexity
    
    def detect_patterns(self, function_code: str) -> Dict[str, bool]:
        """Detect data science patterns in function code."""
        patterns = {
            'data_loading': False,
            'data_preprocessing': False,
            'feature_engineering': False,
            'train_test_split': False,
            'model_training': False,
            'model_evaluation': False,
            'visualization': False
        }
        
        code_lower = function_code.lower()
        
        # Fast pattern matching using string operations
        if any(pattern in code_lower for pattern in ['read_csv', 'read_excel', 'load_data']):
            patterns['data_loading'] = True
        
        if any(pattern in code_lower for pattern in ['dropna', 'fillna', 'drop']):
            patterns['data_preprocessing'] = True
        
        if any(pattern in code_lower for pattern in ['transform', 'scale', 'encode']):
            patterns['feature_engineering'] = True
        
        if 'train_test_split' in code_lower:
            patterns['train_test_split'] = True
        
        if any(pattern in code_lower for pattern in ['fit(', 'train', 'randomforest']):
            patterns['model_training'] = True
        
        if any(pattern in code_lower for pattern in ['score', 'accuracy', 'precision']):
            patterns['model_evaluation'] = True
        
        if any(pattern in code_lower for pattern in ['plot', 'show', 'figure']):
            patterns['visualization'] = True
        
        return patterns


class DataSciencePatternDetector:
    """
    Ultra-fast data science pattern detection using optimized algorithms.
    
    Combines regex, AST analysis, and string matching for maximum speed.
    """
    
    def __init__(self):
        # Pre-compiled patterns for speed
        self._ml_patterns = {
            'data_loading': re.compile(r'(read_csv|read_excel|read_json|read_sql|load_data)', re.I),
            'exploratory_analysis': re.compile(r'(\.info\(\)|\.describe\(\)|\.head\(\)|\.tail\(\))', re.I),
            'data_preprocessing': re.compile(r'(dropna|fillna|drop|get_dummies)', re.I),
            'feature_engineering': re.compile(r'(transform|fit_transform|scale|normalize)', re.I),
            'train_test_split': re.compile(r'train_test_split', re.I),
            'model_training': re.compile(r'(\.fit\(|RandomForest|LogisticRegression|SVM)', re.I),
            'model_evaluation': re.compile(r'(accuracy_score|precision_score|recall_score|f1_score)', re.I),
            'visualization': re.compile(r'(plt\.|plot\(|show\(|figure\()', re.I),
            'model_persistence': re.compile(r'(joblib\.dump|pickle\.dump|save_model)', re.I)
        }
        
        self._dl_patterns = {
            'neural_network_architecture': re.compile(r'(Sequential|Dense|Conv2D|LSTM)', re.I),
            'model_compilation': re.compile(r'(\.compile\(|optimizer|loss|metrics)', re.I),
            'callbacks': re.compile(r'(EarlyStopping|ModelCheckpoint|ReduceLROnPlateau)', re.I),
            'training_history': re.compile(r'(\.fit\(.*epochs|history)', re.I),
            'training_visualization': re.compile(r'(history\.history|plot.*loss|plot.*accuracy)', re.I)
        }
    
    def detect_all_patterns(self, code: str) -> Dict[str, bool]:
        """Detect all data science patterns in code."""
        patterns = {}
        
        # ML patterns
        for pattern_name, regex in self._ml_patterns.items():
            patterns[pattern_name] = bool(regex.search(code))
        
        # DL patterns
        for pattern_name, regex in self._dl_patterns.items():
            patterns[pattern_name] = bool(regex.search(code))
        
        return patterns
    
    def detect_deep_learning_patterns(self, code: str) -> Dict[str, bool]:
        """Detect deep learning specific patterns."""
        patterns = {}
        for pattern_name, regex in self._dl_patterns.items():
            patterns[pattern_name] = bool(regex.search(code))
        return patterns
    
    def detect_quality_issues(self, code: str) -> Dict[str, bool]:
        """Detect potential data quality issues in code."""
        issues = {
            'no_data_validation': True,
            'no_train_test_split': True,
            'no_cross_validation': True,
            'data_leakage_risk': False
        }
        
        code_lower = code.lower()
        
        # Check for data validation
        if any(pattern in code_lower for pattern in ['assert', 'check', 'validate', 'info()', 'describe()']):
            issues['no_data_validation'] = False
        
        # Check for train-test split
        if 'train_test_split' in code_lower:
            issues['no_train_test_split'] = False
        
        # Check for cross-validation
        if any(pattern in code_lower for pattern in ['cross_val', 'kfold', 'stratified']):
            issues['no_cross_validation'] = False
        
        # Check for potential data leakage
        if 'fit(' in code_lower and 'train_test_split' not in code_lower:
            issues['data_leakage_risk'] = True
        
        return issues


class CodeBlockValidator:
    """
    Fast validation of individual code blocks within Python files.
    
    Optimized for processing large files by validating blocks independently.
    """
    
    def validate_imports(self, import_code: str) -> PythonValidationResult:
        """Validate import statements."""
        result = PythonValidationResult(
            is_valid=True,
            file_path='import_block'
        )
        
        try:
            tree = ast.parse(import_code)
            analyzer = FastASTAnalyzer()
            analyzer.visit(tree)
            
            result.imports_found = list(analyzer.imports)
            
        except SyntaxError as e:
            result.add_error(f"Import syntax error: {str(e)}")
            result.has_syntax_errors = True
        
        return result
    
    def validate_function(self, function_code: str) -> PythonValidationResult:
        """Validate function definition."""
        result = PythonValidationResult(
            is_valid=True,
            file_path='function_block'
        )
        
        try:
            tree = ast.parse(function_code)
            
            # Find function node
            func_node = None
            for node in ast.walk(tree):
                if isinstance(node, ast.FunctionDef):
                    func_node = node
                    break
            
            if func_node:
                result.functions_found = [func_node.name]
                result.has_docstring = ast.get_docstring(func_node) is not None
                result.has_type_hints = bool(func_node.returns or any(arg.annotation for arg in func_node.args.args))
                result.has_return_statement = any(isinstance(n, ast.Return) for n in ast.walk(func_node))
                result.complexity_score = self._calculate_complexity(func_node)
            
        except SyntaxError as e:
            result.add_error(f"Function syntax error: {str(e)}")
            result.has_syntax_errors = True
        
        return result
    
    def validate_data_processing(self, processing_code: str) -> PythonValidationResult:
        """Validate data processing code block."""
        result = PythonValidationResult(
            is_valid=True,
            file_path='processing_block'
        )
        
        detector = DataSciencePatternDetector()
        patterns = detector.detect_all_patterns(processing_code)
        
        result.patterns_detected = patterns
        result.has_data_cleaning = patterns.get('data_preprocessing', False)
        result.has_feature_engineering = patterns.get('feature_engineering', False)
        result.has_train_test_split = patterns.get('train_test_split', False)
        result.has_data_science_patterns = any(patterns.values())
        
        return result
    
    def _calculate_complexity(self, node) -> int:
        """Calculate cyclomatic complexity."""
        complexity = 1
        for child in ast.walk(node):
            if isinstance(child, (ast.If, ast.While, ast.For, ast.Try, ast.With)):
                complexity += 1
        return complexity


class PythonFileValidator:
    """
    Main validator for Python files with ultra-fast processing.
    
    Combines all validation components for comprehensive analysis.
    Uses parallel processing and caching for maximum performance.
    """
    
    def __init__(self):
        self.import_validator = ImportValidator()
        self.function_validator = FunctionValidator()
        self.pattern_detector = DataSciencePatternDetector()
        self.code_validator = CodeBlockValidator()
        
        # Performance optimization
        self._file_cache = {}
        self._max_cache_size = 1000
    
    def validate_file(self, file_path: str) -> PythonValidationResult:
        """
        Validate a Python file with comprehensive analysis.
        
        Ultra-fast processing with parallel analysis where possible.
        """
        start_time = time.time()
        
        # Initialize result
        result = PythonValidationResult(
            is_valid=True,
            file_path=file_path
        )
        
        try:
            # Read file efficiently
            path_obj = Path(file_path)
            if not path_obj.exists():
                result.add_error(f"File not found: {file_path}")
                return result
            
            # Get file stats
            result.file_size_bytes = path_obj.stat().st_size
            
            # Check cache
            file_hash = self._get_file_hash(path_obj)
            if file_hash in self._file_cache:
                cached_result = self._file_cache[file_hash]
                cached_result.validation_time = time.time() - start_time
                return cached_result
            
            # Read file content
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                code = f.read()
            
            result.lines_of_code = len(code.split('\n'))
            
            # Parse AST for comprehensive analysis
            try:
                tree = ast.parse(code)
                analyzer = FastASTAnalyzer()
                analyzer.visit(tree)
                
                # Extract results from analyzer
                result.imports_found = list(analyzer.imports)
                result.functions_found = [func['name'] for func in analyzer.functions]
                result.patterns_detected = analyzer.patterns
                result.has_data_science_patterns = any(analyzer.patterns.values())
                
                # Calculate function quality scores
                for func_info in analyzer.functions:
                    func_name = func_info['name']
                    # Extract function code for quality analysis
                    func_code = self._extract_function_code(code, func_name)
                    if func_code:
                        quality_score = self.function_validator.calculate_quality_score(func_code)
                        result.function_quality_scores[func_name] = quality_score
                
            except SyntaxError as e:
                result.add_error(f"Syntax error: {str(e)}")
                result.syntax_errors.append(str(e))
                result.has_syntax_errors = True
            
            # Check for missing imports
            missing_imports = self.import_validator.find_missing_imports(code)
            result.missing_imports = missing_imports
            
            if missing_imports:
                result.add_warning(f"Missing imports detected: {', '.join(missing_imports)}")
            
            # Detect quality issues
            quality_issues = self.pattern_detector.detect_quality_issues(code)
            for issue, has_issue in quality_issues.items():
                if has_issue:
                    result.add_warning(f"Quality issue detected: {issue}")
            
            # Cache result if cache not full
            if len(self._file_cache) < self._max_cache_size:
                self._file_cache[file_hash] = result
            
        except Exception as e:
            result.add_error(f"Validation error: {str(e)}")
        
        result.validation_time = time.time() - start_time
        return result
    
    def validate_multiple_files(self, file_paths: List[str], max_workers: int = 4) -> List[PythonValidationResult]:
        """
        Validate multiple Python files in parallel for maximum speed.
        """
        results = []
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(self.validate_file, path): path for path in file_paths}
            
            for future in as_completed(futures):
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    # Create error result for failed validation
                    error_result = PythonValidationResult(
                        is_valid=False,
                        file_path=futures[future]
                    )
                    error_result.add_error(f"Validation failed: {str(e)}")
                    results.append(error_result)
        
        return results
    
    def _get_file_hash(self, path_obj: Path) -> str:
        """Generate hash for file caching."""
        stat = path_obj.stat()
        return f"{path_obj.name}_{stat.st_size}_{stat.st_mtime}"
    
    def _extract_function_code(self, full_code: str, function_name: str) -> Optional[str]:
        """Extract individual function code for analysis."""
        try:
            tree = ast.parse(full_code)
            
            for node in ast.walk(tree):
                if isinstance(node, ast.FunctionDef) and node.name == function_name:
                    # Get the function's source code
                    lines = full_code.split('\n')
                    start_line = node.lineno - 1
                    
                    # Find end of function (simple heuristic)
                    end_line = start_line + 1
                    indent_level = len(lines[start_line]) - len(lines[start_line].lstrip())
                    
                    for i in range(start_line + 1, len(lines)):
                        line = lines[i]
                        if line.strip() and (len(line) - len(line.lstrip())) <= indent_level:
                            break
                        end_line = i + 1
                    
                    return '\n'.join(lines[start_line:end_line])
            
        except:
            pass
        
        return None